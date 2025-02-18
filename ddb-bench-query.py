import boto3
from boto3.dynamodb.conditions import Key
import threading
import time
import random
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('mvdemo1')

# List to hold oneids
oneids = []

# Shared variables for tracking total queries and items
total_queries = 0
total_items = 0
total_errors = 0
lock = threading.Lock()

# Flag to signal threads to stop
stop_flag = threading.Event()

# Function to scan the table and populate the oneids list
def scan_table(target_size=100000):
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

        time.sleep(0.1)  # Small delay to reduce throttling

    # Remove duplicates while preserving order
    oneids[:] = list(dict.fromkeys(oneids))
    print(f"Scan complete. Total unique oneids: {len(oneids)}")

# Function to query the table
def query_table():
    global total_queries, total_items, total_errors
    while not stop_flag.is_set():
        oneid = random.choice(oneids)
        try:
            response = table.query(
                KeyConditionExpression=Key('oneid').eq(oneid)
            )
            with lock:
                total_queries += 1
                total_items += len(response['Items'])
        except Exception as e:
            with lock:
                total_errors += 1
            print(f"Error querying oneid {oneid}: {str(e)}")

# Number of threads
num_threads = 100  # Increased from 40

# Scan the table first
scan_table()

if not oneids:
    print("No oneids found. Exiting.")
    exit()

# Create and start threads using ThreadPoolExecutor
start_time = time.time()
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    futures = [executor.submit(query_table) for _ in range(num_threads)]

    # Run for 10 minutes
    time.sleep(600)

    # Signal threads to stop
    stop_flag.set()

    # Wait for all threads to complete
    for future in futures:
        future.result()

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
