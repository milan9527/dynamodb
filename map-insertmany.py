#!/usr/bin/env python3
import boto3
import time
import hashlib
import random
import concurrent.futures
import logging
from decimal import Decimal
import uuid
import os
import json
from botocore.exceptions import ClientError
from botocore.config import Config
import threading
import queue
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure boto3 connection pooling
boto_config = Config(
    max_pool_connections=100,  # Significantly increased pool size
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    connect_timeout=5,
    read_timeout=30
)

class MapDynamoDBManager:
    def __init__(self, table_name='mapdemo', region='us-east-1', endpoint_url=None):
        """Initialize the DynamoDB manager"""
        self.table_name = table_name
        self.region = region
        self.endpoint_url = endpoint_url
        self.client_pool = queue.Queue()
        self.client_pool_size = 50  # Number of clients to pre-create
        self._initialize_client_pool()

    def _initialize_client_pool(self):
        """Initialize a pool of DynamoDB clients"""
        for _ in range(self.client_pool_size):
            client = boto3.client(
                'dynamodb',
                region_name=self.region,
                config=boto_config,
                endpoint_url=self.endpoint_url
            )
            self.client_pool.put(client)

    def _get_client(self):
        """Get a client from the pool"""
        try:
            return self.client_pool.get(block=False)
        except queue.Empty:
            # Create a new client if pool is empty
            return boto3.client(
                'dynamodb',
                region_name=self.region,
                config=boto_config,
                endpoint_url=self.endpoint_url
            )

    def _return_client(self, client):
        """Return a client to the pool"""
        try:
            self.client_pool.put(client, block=False)
        except queue.Full:
            # Just discard if pool is full
            pass

    def generate_batch_data(self, start_idx, batch_size, branches, tiles, element_types):
        """Generate a batch of data items - optimized for speed"""
        items = []
        current_time = int(time.time() * 1000)

        for i in range(start_idx, start_idx + batch_size):
            branch_idx = i % len(branches)
            tile_idx = (i // 10) % len(tiles)
            element_type_idx = (i // 100) % len(element_types)

            branch = branches[branch_idx]
            tile_base = tiles[tile_idx]
            # Modified: tile is now in "branch#tile" format
            tile = f"{branch}#{tile_base}"
            element_type = element_types[element_type_idx]
            element_id = str(i % 1000000).zfill(7)
            element = f"{element_type}{element_id}"

            # Generate deterministic element value based on index to avoid MD5 calculation overhead
            element_value = f"value_{i % 10000000}"

            # Create composite partition key
            bte = f"{branch}#{tile_base}#{element}"

            # Use pre-calculated MD5 values for common strings to avoid computation
            element_md5 = hashlib.md5(element_value.encode()).hexdigest()

            # Create the item - using dict comprehension for speed
            item = {
                'bte': {'S': bte},
                'tsver': {'N': str(current_time - (i % 86400000))},
                'branch': {'S': branch},
                'tile': {'S': tile},  # Now in "branch#tile" format
                'element': {'S': element},
                'element_value': {'S': element_value},
                'element_md5': {'S': element_md5}
            }

            items.append({'PutRequest': {'Item': item}})

        return items

    def batch_write_with_retry(self, items, max_retries=5):
        """Write items in batches with retry logic"""
        request_items = {self.table_name: items}

        # Get client from pool
        client = self._get_client()

        try:
            retries = 0
            backoff_time = 50  # Start with 50ms

            while retries < max_retries:
                try:
                    response = client.batch_write_item(RequestItems=request_items)
                    unprocessed = response.get('UnprocessedItems', {})

                    if not unprocessed or not unprocessed.get(self.table_name):
                        return True

                    # If there are unprocessed items, retry with backoff
                    request_items = unprocessed
                    retries += 1

                    if retries < max_retries:
                        time.sleep(backoff_time / 1000.0)  # Convert ms to seconds
                        backoff_time = min(backoff_time * 2, 1000)  # Exponential backoff capped at 1 second

                except ClientError as e:
                    error_code = e.response['Error']['Code']

                    # Handle provisioned throughput exceeded with backoff
                    if error_code == 'ProvisionedThroughputExceededException':
                        retries += 1
                        if retries < max_retries:
                            time.sleep(backoff_time / 1000.0)
                            backoff_time = min(backoff_time * 2, 2000)
                    else:
                        logger.error(f"Batch write error: {e}")
                        retries += 1
                        if retries >= max_retries:
                            return False
                        time.sleep(backoff_time / 1000.0)

            return False
        finally:
            # Return client to pool
            self._return_client(client)

    def parallel_insert_large_dataset(self, total_items, batch_size=25, max_workers=50):
        """Insert a large dataset in parallel with optimized throughput"""
        logger.info(f"Starting insertion of {total_items:,} items")

        # Pre-generate reference data
        branches = [f"branch{i}" for i in range(10)]
        tiles = [f"t{str(i).zfill(5)}" for i in range(1000)]
        element_types = ["rd", "poi", "bld", "lnd", "trf"]

        # Create progress tracking file
        progress_file = "dynamodb_insert_progress.json"
        if os.path.exists(progress_file):
            with open(progress_file, 'r') as f:
                progress = json.load(f)
                start_idx = progress.get('last_index', 0)
                logger.info(f"Resuming from index {start_idx:,}")
        else:
            start_idx = 0

        # Stats tracking
        start_time = time.time()
        last_log_time = start_time
        items_inserted = 0
        last_items_inserted = 0
        stats_lock = threading.Lock()

        # Batch queue for better control
        batch_queue = queue.Queue(maxsize=max_workers * 2)
        done_event = threading.Event()

        # Producer thread to generate batches
        def batch_producer():
            idx = start_idx
            while idx < total_items and not done_event.is_set():
                current_batch_size = min(batch_size, total_items - idx)
                batch = self.generate_batch_data(idx, current_batch_size, branches, tiles, element_types)
                batch_queue.put((batch, idx, current_batch_size))
                idx += current_batch_size

            # Signal completion
            batch_queue.put(None)

        # Start producer thread
        producer_thread = threading.Thread(target=batch_producer)
        producer_thread.daemon = True
        producer_thread.start()

        # Consumer function for worker threads
        def batch_consumer():
            nonlocal items_inserted

            while not done_event.is_set():
                try:
                    item = batch_queue.get(timeout=1)
                    if item is None:  # End signal
                        batch_queue.put(None)  # Propagate end signal
                        break

                    batch, idx, size = item
                    success = self.batch_write_with_retry(batch)

                    with stats_lock:
                        if success:
                            items_inserted += size
                        else:
                            logger.warning(f"Failed to insert batch at index {idx}")

                    batch_queue.task_done()
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Consumer error: {e}")

        # Start consumer threads
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            consumers = [executor.submit(batch_consumer) for _ in range(max_workers)]

            try:
                # Monitor progress and throughput
                while items_inserted < total_items and not done_event.is_set():
                    time.sleep(5)
                    current_time = time.time()
                    elapsed = current_time - last_log_time

                    with stats_lock:
                        items_since_last = items_inserted - last_items_inserted

                    if elapsed > 0:
                        rate = items_since_last / elapsed
                        overall_rate = items_inserted / (current_time - start_time)
                        percent_complete = (items_inserted / total_items) * 100
                        eta_seconds = (total_items - items_inserted) / overall_rate if overall_rate > 0 else 0

                        logger.info(f"Progress: {items_inserted:,}/{total_items:,} ({percent_complete:.2f}%) | "
                                   f"Current rate: {rate:.2f} items/sec | "
                                   f"Overall rate: {overall_rate:.2f} items/sec | "
                                   f"ETA: {eta_seconds/60:.1f} minutes")

                        last_log_time = current_time
                        last_items_inserted = items_inserted

                        # Save progress
                        with open(progress_file, 'w') as f:
                            json.dump({'last_index': start_idx + items_inserted}, f)

            except KeyboardInterrupt:
                logger.info("Interrupted. Shutting down gracefully...")
                done_event.set()

                # Save progress before exiting
                with open(progress_file, 'w') as f:
                    json.dump({'last_index': start_idx + items_inserted}, f)

            # Wait for all consumers to finish
            for future in consumers:
                try:
                    future.result(timeout=10)
                except concurrent.futures.TimeoutError:
                    logger.warning("Timeout waiting for worker to complete")

        # Final stats
        total_time = time.time() - start_time
        final_rate = items_inserted / total_time if total_time > 0 else 0

        logger.info(f"Insertion complete: {items_inserted:,} items in {total_time:.2f} seconds")
        logger.info(f"Overall throughput: {final_rate:.2f} items/sec")

        return items_inserted

def main():
    # Create manager for existing table in us-east-1
    manager = MapDynamoDBManager(table_name='mapdemo', region='us-east-1')

    # Insert 100 million items with aggressive parallelism
    total_items = 100000000
    batch_size = 25  # DynamoDB batch_write_item limit is 25 items
    max_workers = 50  # Increased for maximum throughput

    logger.info(f"Starting data insertion of {total_items:,} items")
    manager.parallel_insert_large_dataset(total_items, batch_size, max_workers)

if __name__ == "__main__":
    main()
