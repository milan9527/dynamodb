import boto3
import time
import random
import json
from concurrent.futures import ThreadPoolExecutor
from botocore.config import Config

# Configure boto3 for maximum performance
config = Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=50
)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', config=config)
table = dynamodb.Table('mydemo')
client = boto3.client('dynamodb', region_name='us-east-1', config=config)

# File to read items from
ITEMS_FILE = 'dynamodb_items.json'

# Shared variables for tracking total queries and items
total_queries = 0
total_items = 0
total_errors = 0

def load_items():
    with open(ITEMS_FILE, 'r') as f:
        return json.load(f)

def query_batch(items, batch_size=100):
    global total_queries, total_items, total_errors
    batch = random.sample(items, min(batch_size, len(items)))
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
        total_queries += len(batch)
        total_items += len(response['Responses'][table.name])
    except Exception as e:
        total_errors += len(batch)
        print(f"Error in batch query: {str(e)}")

def run_benchmark(items, duration, num_threads):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        end_time = time.time() + duration
        while time.time() < end_time:
            futures = [executor.submit(query_batch, items) for _ in range(num_threads)]
            for future in futures:
                future.result()

if __name__ == "__main__":
    # Load items from file
    items = load_items()

    if not items:
        print("No items found. Make sure to run scan_and_save.py first.")
        exit()

    # Run benchmark
    print("Starting benchmark...")
    num_threads = 1000  # Adjust based on your needs
    duration = 600  # 10 minutes

    start_time = time.time()
    run_benchmark(items, duration, num_threads)
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
