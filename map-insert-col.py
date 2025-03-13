import boto3
import time
import random
from datetime import datetime, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

# Constants
TABLE_NAME = 'ElementGeoLocationData'
TOTAL_ITEMS = 100000  # 100,000 items total
NUM_ELEMENTS = 100    # Approximately 100 elements
NUM_BLOCKS = 50
STATUS_VALUES = ['ACTIVE', 'INACTIVE', 'MAINTENANCE', 'PENDING']
COLUMNS_PER_ELEMENT = 5  # Each element has exactly 5 columns
VERSIONS_PER_ELEMENT_COLUMN = 200  # Increased to get ~100,000 items from 100 elements with 5 columns each

def create_table(dynamodb_client):
    """Create the DynamoDB table with the specified schema using on-demand capacity"""
    try:
        print(f"Creating table: {TABLE_NAME}")
        
        # Check if table already exists
        existing_tables = dynamodb_client.list_tables()['TableNames']
        if TABLE_NAME in existing_tables:
            print(f"Table {TABLE_NAME} already exists. Skipping creation.")
            return
        
        # Create table with on-demand capacity
        response = dynamodb_client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'element_col_id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'element_col_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'N'
                },
                {
                    'AttributeName': 'block_id',
                    'AttributeType': 'S'
                }
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'BlockIndex',
                    'KeySchema': [
                        {
                            'AttributeName': 'block_id',
                            'KeyType': 'HASH'
                        },
                        {
                            'AttributeName': 'timestamp',
                            'KeyType': 'RANGE'
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'
                    }
                }
            ],
            BillingMode='PAY_PER_REQUEST'  # On-demand capacity mode
        )
        
        # Wait for table to be created
        print("Waiting for table to become ACTIVE...")
        waiter = dynamodb_client.get_waiter('table_exists')
        waiter.wait(TableName=TABLE_NAME)
        print("Table created successfully.")
        
    except ClientError as e:
        print(f"Error creating table: {e}")
        raise e

def generate_random_item(element_id, column_id, version, base_timestamp):
    """Generate a random item for the DynamoDB table"""
    element_col_id = f"ele{element_id}#col{column_id}"
    
    block_id = f"block{random.randint(1, NUM_BLOCKS)}"
    status = random.choice(STATUS_VALUES)
    
    # Each version is at least 1 second newer than the previous one
    # to ensure unique timestamps for the same element_col_id
    timestamp = base_timestamp + version
    
    # Generate random geo location
    latitude = Decimal(str(random.uniform(24.0, 49.0)))  # US latitude range
    longitude = Decimal(str(random.uniform(-125.0, -66.0)))  # US longitude range
    
    # Create the item
    item = {
        'element_col_id': element_col_id,
        'timestamp': timestamp,
        'block_id': block_id,
        'status': status,
        'geo_location': {
            'latitude': latitude,
            'longitude': longitude,
            'accuracy': Decimal(str(random.uniform(1.0, 10.0)))
        },
        'metadata': {
            'reading': Decimal(str(random.uniform(0, 100))),
            'unit': 'meters',
            'quality': Decimal(str(random.uniform(0, 1))),
            'version': version
        },
        'element_id': f"ele{element_id}",
        'column_id': column_id,
        'source': f"sensor{random.randint(1, 100)}",
        'is_valid': random.choice([True, False]),
        'last_updated': datetime.now().isoformat()
    }
    
    return item

def insert_items(dynamodb_resource):
    """Insert items into the DynamoDB table"""
    try:
        table = dynamodb_resource.Table(TABLE_NAME)
        print(f"Inserting approximately {TOTAL_ITEMS} items into the table...")
        
        # For 100 elements with 5 columns each, we need ~200 versions per element-column
        # to reach 100,000 items: 100 * 5 * 200 = 100,000
        
        items_inserted = 0
        batch_size = 25  # DynamoDB batch write limit
        start_time = time.time()
        last_progress_time = start_time
        
        # Create a distribution of versions across elements to reach our target
        for element_id in range(1, NUM_ELEMENTS + 1):
            # Each element has exactly 5 columns
            for column_id in range(1, COLUMNS_PER_ELEMENT + 1):
                # Generate a base timestamp for this element-column combination
                # Base timestamp is between 1-365 days ago to spread data over a year
                base_days_ago = random.randint(1, 365)
                base_timestamp = int((datetime.now() - timedelta(days=base_days_ago)).timestamp())
                
                # Calculate versions needed for this element-column
                # Add some randomness but ensure we'll reach our target
                versions_for_this_combo = max(1, int(random.gauss(VERSIONS_PER_ELEMENT_COLUMN, 20)))
                
                # Process versions in smaller chunks to avoid memory issues
                batch_items = []
                
                for version in range(versions_for_this_combo):
                    # Stop if we've reached the total
                    if items_inserted >= TOTAL_ITEMS:
                        break
                        
                    item = generate_random_item(element_id, column_id, version, base_timestamp)
                    batch_items.append(item)
                    items_inserted += 1
                    
                    # If we've reached the batch size, write the batch
                    if len(batch_items) >= batch_size:
                        with table.batch_writer() as batch:
                            for batch_item in batch_items:
                                batch.put_item(Item=batch_item)
                        
                        batch_items = []
                        
                        # Print progress every 5 seconds or every 5000 items
                        current_time = time.time()
                        if items_inserted % 5000 == 0 or (current_time - last_progress_time) >= 5:
                            elapsed = current_time - start_time
                            items_per_second = items_inserted / elapsed if elapsed > 0 else 0
                            print(f"Inserted {items_inserted} items... ({items_per_second:.2f} items/sec, {items_inserted/TOTAL_ITEMS*100:.1f}%)")
                            last_progress_time = current_time
                
                # Write any remaining items for this element-column
                if batch_items:
                    with table.batch_writer() as batch:
                        for batch_item in batch_items:
                            batch.put_item(Item=batch_item)
                    
                    # Print progress
                    current_time = time.time()
                    if (current_time - last_progress_time) >= 5:
                        elapsed = current_time - start_time
                        items_per_second = items_inserted / elapsed if elapsed > 0 else 0
                        print(f"Inserted {items_inserted} items... ({items_per_second:.2f} items/sec, {items_inserted/TOTAL_ITEMS*100:.1f}%)")
                        last_progress_time = current_time
                
                if items_inserted >= TOTAL_ITEMS:
                    break
            
            if items_inserted >= TOTAL_ITEMS:
                break
        
        total_time = time.time() - start_time
        print(f"Successfully inserted {items_inserted} items in {total_time:.2f} seconds.")
        print(f"Average throughput: {items_inserted / total_time:.2f} items/second.")
        
    except ClientError as e:
        print(f"Error inserting items: {e}")
        raise e

def main():
    """Main function to create table and insert items"""
    start_time = time.time()
    
    # Initialize DynamoDB client and resource
    dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')
    dynamodb_resource = boto3.resource('dynamodb', region_name='us-east-1')
    
    # Create table
    create_table(dynamodb_client)
    
    # Insert items
    insert_items(dynamodb_resource)
    
    total_time = time.time() - start_time
    print(f"Table creation and data insertion completed in {total_time:.2f} seconds.")

if __name__ == "__main__":
    main()
