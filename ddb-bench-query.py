import boto3
import time
import random
from concurrent.futures import ThreadPoolExecutor
import threading

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('mvdemo1')
client = boto3.client('dynamodb', region_name='us-east-1')

# List to hold oneids
oneids = []

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

    while len(oneids) < target_size:
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        oneids.extend([item['oneid'] for item in response['Items']])
        last_evaluated_key = response.get('LastEvaluatedKey')

        print(f"Scanned {len(oneids)} oneids")

        if not last_evaluated_key:
            break

    # Remove duplicates while preserving order
    oneids[:] = list(dict.fromkeys(oneids))
    print(f"Scan complete. Total unique oneids: {len(oneids)}")

def query_batch(batch_size=100):
    global total_queries, total_items, total_errors
    batch = random.sample(oneids, min(batch_size, len(oneids)))
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

def run_benchmark(duration, num_threads):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        end_time = time.time() + duration
        while time.time() < end_time:
            futures = [executor.submit(query_batch) for _ in range(num_threads)]
            for future in futures:
                future.result()

# Scan the table first
scan_table()

if not oneids:
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
