import boto3
import time
import json
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config
import threading

# Configure boto3 for maximum performance
config = Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=100
)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', config=config)
table = dynamodb.Table('mydemo')
client = boto3.client('dynamodb', region_name='us-east-1', config=config)

# File to read items from
ITEMS_FILE = 'dynamodb_items.json'

# Thread-local storage for performance metrics
thread_local = threading.local()

def load_items():
    with open(ITEMS_FILE, 'r') as f:
        return json.load(f)

def split_items(items, num_threads):
    items_per_thread = len(items) // num_threads
    return [items[i:i + items_per_thread] for i in range(0, len(items), items_per_thread)]

def query_batch(items, start_index, batch_size=100):
    end_index = min(start_index + batch_size, len(items))
    batch = items[start_index:end_index]
    try:
        response = client.batch_get_item(
            RequestItems={
                table.name: {
                    'Keys': [{'oneid': {'S': item['oneid']}, 'type': {'S': item['type']}} for item in batch],
                    'ProjectionExpression': 'oneid, #t',
                    'ExpressionAttributeNames': {'#t': 'type'}
                }
            }
        )
        retrieved_items = len(response['Responses'][table.name])
        if not hasattr(thread_local, 'stats'):
            thread_local.stats = {'queries': 0, 'items': 0, 'errors': 0}
        thread_local.stats['queries'] += len(batch)
        thread_local.stats['items'] += retrieved_items
    except Exception as e:
        if not hasattr(thread_local, 'stats'):
            thread_local.stats = {'queries': 0, 'items': 0, 'errors': 0}
        thread_local.stats['errors'] += len(batch)
        print(f"Error in batch query: {str(e)}")
    return end_index

def worker(items, end_time):
    start_index = 0
    while time.time() < end_time:
        start_index = query_batch(items, start_index)
        if start_index >= len(items):
            start_index = 0  # Reset to beginning if we've reached the end

def run_benchmark(item_chunks, duration, num_threads):
    end_time = time.time() + duration
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, chunk, end_time) for chunk in item_chunks]
        for future in futures:
            future.result()

if __name__ == "__main__":
    # Load items from file
    items = load_items()

    if not items:
        print("No items found. Make sure to run scan_and_save.py first.")
        exit()

    print(f"Loaded {len(items)} items from file.")

    # Run benchmark
    print("Starting benchmark...")
    num_threads = 2000  # Adjust based on your needs
    duration = 600  # 10 minutes

    # Split items among threads
    item_chunks = split_items(items, num_threads)

    start_time = time.time()
    run_benchmark(item_chunks, duration, num_threads)
    end_time = time.time()

    # Aggregate results
    total_queries = sum(getattr(thread, 'stats', {}).get('queries', 0) for thread in threading.enumerate())
    total_items = sum(getattr(thread, 'stats', {}).get('items', 0) for thread in threading.enumerate())
    total_errors = sum(getattr(thread, 'stats', {}).get('errors', 0) for thread in threading.enumerate())

    # Calculate and print results
    total_time = end_time - start_time
    requests_per_second = total_queries / total_time

    print(f"\nBenchmark completed:")
    print(f"Total queries: {total_queries}")
    print(f"Total items retrieved: {total_items}")
    print(f"Total errors: {total_errors}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"Requests per second: {requests_per_second:.2f}")
