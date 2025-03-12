import boto3
import time
import random
import uuid
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor
import math

class MapBulkLoader:
    """Class to handle bulk loading of map elements into DynamoDB"""
    
    def __init__(self, region='us-east-1', table_name='map'):
        """Initialize the DynamoDB client and set table name"""
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
        
        # For direct batch writing
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)
    
    def generate_random_map_element(self, index):
        """Generate a random map element with realistic data"""
        # Define possible values for each attribute
        blocks = [
            "downtown_block_1", "downtown_block_2", "residential_zone_a", 
            "industrial_area_b", "commercial_district_c", "suburban_area_d",
            "historic_district_e", "waterfront_zone_f", "university_campus_g",
            "park_area_h"
        ]
        
        versions = ["1.0", "1.1", "1.5", "2.0", "2.1", "3.0"]
        statuses = ["active", "inactive", "under_construction", "maintenance", "planned"]
        
        # Generate a unique element ID
        ele_id = f"element_{index}_{uuid.uuid4().hex[:8]}"
        
        # Select a block based on index to ensure some elements share the same block
        block_index = index % len(blocks)
        block = blocks[block_index]
        
        # Generate other attributes
        version = random.choice(versions)
        status = random.choice(statuses)
        
        # Generate a realistic attitude value (elevation in meters)
        attitude = round(random.uniform(0, 1000), 2)
        
        # Generate timestamp (slightly in the past to ensure uniqueness)
        timestamp = int(time.time() * 1000) - random.randint(0, 10000)
        
        return {
            'ele': ele_id,
            'timestamp': timestamp,
            'block': block,
            'version': version,
            'status': status,
            'attitude': attitude
        }
    
    def batch_write_with_retries(self, items_batch):
        """Write a batch of items with exponential backoff for throttling"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Convert items to DynamoDB format
                request_items = {
                    self.table_name: [{'PutRequest': {'Item': self._convert_to_dynamodb_format(item)}} 
                                     for item in items_batch]
                }
                
                response = self.dynamodb_client.batch_write_item(RequestItems=request_items)
                
                # Check for unprocessed items
                unprocessed = response.get('UnprocessedItems', {}).get(self.table_name, [])
                if not unprocessed:
                    return True
                
                # If there are unprocessed items, retry with those items
                items_batch = [self._convert_from_dynamodb_format(item['PutRequest']['Item']) 
                              for item in unprocessed]
                
                # Exponential backoff
                wait_time = (2 ** retry_count) * 100  # milliseconds
                time.sleep(wait_time / 1000.0)
                retry_count += 1
            
            except ClientError as e:
                if e.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                    # Exponential backoff for throughput exceptions
                    wait_time = (2 ** retry_count) * 100
                    time.sleep(wait_time / 1000.0)
                    retry_count += 1
                else:
                    print(f"Error in batch write: {e}")
                    return False
        
        print(f"Failed to write {len(items_batch)} items after {max_retries} retries")
        return False
    
    def _convert_to_dynamodb_format(self, item):
        """Convert a Python dict to DynamoDB format"""
        dynamodb_item = {}
        for key, value in item.items():
            if isinstance(value, str):
                dynamodb_item[key] = {'S': value}
            elif isinstance(value, int):
                dynamodb_item[key] = {'N': str(value)}
            elif isinstance(value, float):
                dynamodb_item[key] = {'N': str(value)}
            # Add more type conversions as needed
        return dynamodb_item
    
    def _convert_from_dynamodb_format(self, dynamodb_item):
        """Convert from DynamoDB format back to Python dict"""
        item = {}
        for key, value_dict in dynamodb_item.items():
            value_type = list(value_dict.keys())[0]
            if value_type == 'S':
                item[key] = value_dict['S']
            elif value_type == 'N':
                # Try to convert to int if possible, otherwise float
                try:
                    item[key] = int(value_dict['N'])
                except ValueError:
                    item[key] = float(value_dict['N'])
            # Add more type conversions as needed
        return item
    
    def insert_bulk_data(self, count=10000, batch_size=25, max_workers=10):
        """
        Insert a specified number of random map elements
        
        Parameters:
        - count: Number of items to insert
        - batch_size: Number of items per batch (max 25 for DynamoDB)
        - max_workers: Number of parallel threads
        
        Returns:
        - tuple: (success_count, failure_count)
        """
        start_time = time.time()
        print(f"Starting bulk insert of {count} items...")
        
        # Generate all items first
        all_items = [self.generate_random_map_element(i) for i in range(count)]
        
        # Split into batches (max 25 items per batch for DynamoDB)
        batches = [all_items[i:i+batch_size] for i in range(0, len(all_items), batch_size)]
        
        success_count = 0
        failure_count = 0
        
        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.batch_write_with_retries, batches))
            
            # Count successes and failures
            for result in results:
                if result:
                    success_count += 1
                else:
                    failure_count += 1
        
        end_time = time.time()
        duration = end_time - start_time
        items_per_second = count / duration
        
        print(f"Bulk insert completed in {duration:.2f} seconds")
        print(f"Successfully processed {success_count} of {len(batches)} batches")
        print(f"Failed to process {failure_count} batches")
        print(f"Insertion rate: {items_per_second:.2f} items per second")
        
        return (success_count * batch_size, failure_count * batch_size)
    
    def insert_bulk_data_with_table_monitoring(self, count=10000, batch_size=25, max_workers=10):
        """Insert bulk data while monitoring table capacity"""
        # Get table description to check if it's on-demand or provisioned
        client = boto3.client('dynamodb', region_name='us-east-1')
        table_desc = client.describe_table(TableName=self.table_name)['Table']
        
        billing_mode = table_desc.get('BillingModeSummary', {}).get('BillingMode', 'PROVISIONED')
        
        if billing_mode == 'PROVISIONED':
            print("Table is using PROVISIONED capacity. Consider switching to PAY_PER_REQUEST for bulk loading.")
            write_capacity = table_desc['ProvisionedThroughput']['WriteCapacityUnits']
            print(f"Current write capacity: {write_capacity} WCU")
            
            # Adjust batch size and workers based on provisioned capacity
            recommended_items_per_second = write_capacity * 0.8  # Use 80% of capacity
            recommended_batch_size = min(25, max(1, int(recommended_items_per_second / max_workers)))
            
            print(f"Recommended batch size: {recommended_batch_size}")
            if recommended_batch_size < batch_size:
                print(f"Adjusting batch size from {batch_size} to {recommended_batch_size}")
                batch_size = recommended_batch_size
        else:
            print("Table is using on-demand capacity (PAY_PER_REQUEST).")
        
        return self.insert_bulk_data(count, batch_size, max_workers)


def main():
    """Main function to demonstrate bulk loading"""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Bulk load map elements into DynamoDB')
    parser.add_argument('--count', type=int, default=10000, help='Number of items to insert')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size (max 25)')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker threads')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--table', type=str, default='map', help='DynamoDB table name')
    
    args = parser.parse_args()
    
    # Initialize the loader
    loader = MapBulkLoader(region=args.region, table_name=args.table)
    
    # Insert the data
    loader.insert_bulk_data_with_table_monitoring(
        count=args.count,
        batch_size=min(25, args.batch_size),  # Ensure batch size doesn't exceed 25
        max_workers=args.workers
    )


if __name__ == "__main__":
    main()
