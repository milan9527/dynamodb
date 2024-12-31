import boto3
import time
import uuid
import random
from faker import Faker

# Initialize Faker for generating realistic dummy data
fake = Faker()

# Initialize DynamoDB clients for both regions
dynamodb_east = boto3.resource('dynamodb', region_name='us-east-1')
dynamodb_west = boto3.resource('dynamodb', region_name='us-west-2')
table_east = dynamodb_east.Table('mrsc')
table_west = dynamodb_west.Table('mrsc')

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

# Function to write and read in us-east-1, then read in us-west-2
def write_and_read_multi_region():
    item = generate_item()
    
    # Write the item in us-east-1
    write_start = time.time()
    table_east.put_item(Item=item)
    write_end = time.time()
    write_time_east = write_end - write_start
    
    # Read the item in us-east-1
    read_start_east = time.time()
    response_east = table_east.get_item(Key={'id': item['id']})
    read_end_east = time.time()
    read_time_east = read_end_east - read_start_east
    
    # Read the item in us-west-2
    read_start_west = time.time()
    response_west = table_west.get_item(Key={'id': item['id']})
    read_end_west = time.time()
    read_time_west = read_end_west - read_start_west
    
    return write_time_east, read_time_east, read_time_west

# Main function
def main():
    start_time = time.time()

    total_items = 1000
    total_write_time_east = 0
    total_read_time_east = 0
    total_read_time_west = 0

    # Process items sequentially
    for _ in range(total_items):
        write_time_east, read_time_east, read_time_west = write_and_read_multi_region()
        total_write_time_east += write_time_east
        total_read_time_east += read_time_east
        total_read_time_west += read_time_west

    end_time = time.time()
    total_execution_time = end_time - start_time

    print(f"Total execution time: {total_execution_time:.2f} seconds")
    print(f"Total write time (us-east-1): {total_write_time_east:.2f} seconds")
    print(f"Total read time (us-east-1): {total_read_time_east:.2f} seconds")
    print(f"Total read time (us-west-2): {total_read_time_west:.2f} seconds")
    print(f"Average write time per item (us-east-1): {total_write_time_east/total_items:.4f} seconds")
    print(f"Average read time per item (us-east-1): {total_read_time_east/total_items:.4f} seconds")
    print(f"Average read time per item (us-west-2): {total_read_time_west/total_items:.4f} seconds")
    print(f"Total items processed: {total_items}")

if __name__ == "__main__":
    main()
