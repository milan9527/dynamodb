import boto3
import time
import uuid
import random
from faker import Faker
from concurrent.futures import ThreadPoolExecutor, as_completed

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('mrsc')

# Initialize Faker for generating realistic dummy data
fake = Faker()

# Function to generate a dummy item
def generate_item():
    return {
        'id': str(uuid.uuid4()),
        'name': fake.name(),
        'email': fake.email(),
        'age': random.randint(18, 80),
        'city': fake.city(),
        'country': fake.country(),
        'job': fake.job(),
        'phone': fake.phone_number(),
        'created_at': fake.date_time_this_year().isoformat(),
        'is_active': random.choice([True, False])
    }

# Function to write a single item
def write_item():
    item = generate_item()
    table.put_item(Item=item)

# Main function
def main():
    start_time = time.time()

    total_items = 40000

    # Use ThreadPoolExecutor for parallel execution
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(write_item) for _ in range(total_items)]
        
        # Wait for all futures to complete
        for future in as_completed(futures):
            future.result()

    end_time = time.time()
    execution_time = end_time - start_time

    print(f"Execution time: {execution_time:.2f} seconds")
    print(f"Total items written: {total_items}")

if __name__ == "__main__":
    main()
