import boto3
import time
import uuid
import random
from faker import Faker

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('msrc1')

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

# Function to write a single item and return the time taken
def write_item():
    item = generate_item()
    start_time = time.time()
    table.put_item(Item=item)
    end_time = time.time()
    return end_time - start_time

# Main function
def main():
    start_time = time.time()

    total_items = 1000
    total_write_time = 0

    # Process items sequentially
    for _ in range(total_items):
        write_time = write_item()
        total_write_time += write_time

    end_time = time.time()
    execution_time = end_time - start_time

    # Calculate average response time
    average_response_time = total_write_time / total_items

    print(f"Total execution time: {execution_time:.2f} seconds")
    print(f"Total write time: {total_write_time:.2f} seconds")
    print(f"Average response time: {average_response_time:.4f} seconds")
    print(f"Total items written: {total_items}")

if __name__ == "__main__":
    main()
