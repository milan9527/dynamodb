import boto3
import time
import random
from concurrent.futures import ThreadPoolExecutor
import threading
import queue

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('mvdemo1')
client = boto3.client('dynamodb', region_name='us-east-1')

# Queue to hold oneids
oneid_queue = queue.Queue()

# Shared variables for tracking total queries and items
total_queries = 0
total_items = 0
total_errors = 0
lock = threading.Lock()

def scan_table(target_size=1000000):
    print("Scanning table...")
    scan_kwargs = {
        'ProjectionExpression': 'oneid',
        'Limit': 1000
    }
    last_evaluated_key = None
    scanned_count = 0

    while scanned_count < target_size:
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        for item in response['Items']:
            oneid_queue.put(item['oneid'])
        scanned_count += len(response['Items'])
        last_evaluated_key = response.get('LastEvaluatedKey')

        if not last_evaluated_key:
            break

    print(f"Scan complete. Total oneids: {scanned_count}")

def query_batch(batch_size=100):
    global total_queries, total_items, total_errors
    batch = []
    for _ in range(batch_size):
        try:
            batch.append(oneid_queue.get_nowait())
        except queue.Empty:
            break

    if not batch:
        return

    try:
        response = client.batch_get_item(
            RequestItems={
                table.name: {
                    'Keys': [{'oneid': {'S': oneid}} for oneid in batch],
                    'ProjectionExpression': 'oneid'
                }
            }
        )
        with lock:
            total_queries += len(batch)
            total_items += len(response['Responses'][table.name])
    except Exception as e:
        with lock:
            total_errors += len(batch)
        print(f"Error in batch query: {str(e)}")

def worker(stop_event):
    while not stop_event.is_set():
        query_batch()

def run_benchmark(duration, num_threads):
    stop_event = threading.Event()
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(worker, stop_event) for _ in range(num_threads)]
        time.sleep(duration)
        stop_event.set()
        for future in futures:
            future.result()

# Scan the table first
scan_table()

if oneid_queue.empty():
    print("No oneids found. Exiting.")
    exit()

# Run benchmark
print("Starting benchmark...")
num_threads = 200  # Adjust based on your needs
duration = 600  # 10 minutes

start_time = time.time()
run_benchmark(duration, num_threads)
end_time = time.time()

# Calculate and print results
total_time = end_time - start_time
requests_per_second = total_queries / total_time

print(f"\nBenchmark completed:")
print(f"Total queries: {total_queries}")
print(f"Total items retrieved: {total_items}")
print(f"Total errors: {total_errors}")
print(f"Total execution time: {total_time:.2f} seconds")
print(f"Requests per second: {requests_per_second:.2f}")
