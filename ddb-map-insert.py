import boto3
import time
import random
import uuid
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor

class MapBulkLoader:
    """Class to handle bulk loading of map elements into DynamoDB"""

    def __init__(self, region='us-east-1', table_name='map'):
        """Initialize the DynamoDB client and set table name"""
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name

        # For direct batch writing
        self.dynamodb_client = boto3.client('dynamodb', region_name=region)

    def generate_random_geolocation(self):
        """Generate a random geolocation (latitude, longitude)"""
        # Generate realistic latitude (-90 to 90) and longitude (-180 to 180)
        latitude = round(random.uniform(-90, 90), 6)
        longitude = round(random.uniform(-180, 180), 6)
        return f"{latitude},{longitude}"

    def generate_random_version(self):
        """Generate a random version number in format X.Y.Z"""
        major = random.randint(1, 10)
        minor = random.randint(0, 99)
        patch = random.randint(0, 999)
        return f"{major}.{minor}.{patch}"

    def generate_random_map_element(self, index):
        """Generate a random map element with realistic data including multiple versions"""
        # Define possible values for each attribute
        blocks = [
            "downtown_block_1", "downtown_block_2", "residential_zone_a",
            "industrial_area_b", "commercial_district_c", "suburban_area_d",
            "historic_district_e", "waterfront_zone_f", "university_campus_g",
            "park_area_h"
        ]

        statuses = ["active", "inactive", "under_construction", "maintenance", "planned"]

        # Generate a unique element ID
        ele_id = f"element_{index}_{uuid.uuid4().hex[:8]}"

        # Select a block based on index to ensure some elements share the same block
        block_index = index % len(blocks)
        block = blocks[block_index]

        # Generate base timestamp (slightly in the past)
        base_timestamp = int(time.time() * 1000) - random.randint(0, 10000)

        # Determine number of versions for this element (1-5)
        num_versions = random.randint(1, 5)

        # Generate multiple versions of the element
        versions = []
        for v_idx in range(num_versions):
            # Generate random version string instead of sequential
            version_num = self.generate_random_version()

            # Each version has its own timestamp, status, attitude, and geolocation
            version_timestamp = base_timestamp - (num_versions - v_idx) * 86400000  # 1 day earlier per version
            status = random.choice(statuses)
            attitude = round(random.uniform(0, 1000), 2)
            geolocation = self.generate_random_geolocation()

            version_data = {
                'ele': ele_id,
                'timestamp': version_timestamp,
                'block': block,
                'version': version_num,
                'status': status,
                'attitude': attitude,
                'geolocation': geolocation
            }
            versions.append(version_data)

        return versions

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
        Insert a specified number of random map elements with multiple versions

        Parameters:
        - count: Number of unique elements to insert
        - batch_size: Number of items per batch (max 25 for DynamoDB)
        - max_workers: Number of parallel threads

        Returns:
        - tuple: (success_count, failure_count)
        """
        start_time = time.time()
        print(f"Starting bulk insert of {count} elements with multiple versions...")

        # Generate all items first (each element may have multiple versions)
        all_versions = []
        for i in range(count):
            element_versions = self.generate_random_map_element(i)
            all_versions.extend(element_versions)

        total_items = len(all_versions)
        print(f"Generated {total_items} total items for {count} elements")

        # Split into batches (max 25 items per batch for DynamoDB)
        batches = [all_versions[i:i+batch_size] for i in range(0, len(all_versions), batch_size)]

        success_count = 0
        failure_count = 0

        # Process batches in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(self.batch_write_with_retries, batches))

            # Count successes and failures
            for i, result in enumerate(results):
                batch_size_actual = len(batches[i])
                if result:
                    success_count += batch_size_actual
                else:
                    failure_count += batch_size_actual

        end_time = time.time()
        duration = end_time - start_time
        items_per_second = total_items / duration

        print(f"Bulk insert completed in {duration:.2f} seconds")
        print(f"Successfully inserted {success_count} of {total_items} items")
        print(f"Failed to insert {failure_count} items")
        print(f"Insertion rate: {items_per_second:.2f} items per second")

        return (success_count, failure_count)


def main():
    """Main function to demonstrate bulk loading"""
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Bulk load map elements into DynamoDB')
    parser.add_argument('--count', type=int, default=10000, help='Number of unique elements to insert')
    parser.add_argument('--batch-size', type=int, default=25, help='Batch size (max 25)')
    parser.add_argument('--workers', type=int, default=10, help='Number of worker threads')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--table', type=str, default='map', help='DynamoDB table name')

    args = parser.parse_args()

    # Initialize the loader
    loader = MapBulkLoader(region=args.region, table_name=args.table)

    # Insert the data
    loader.insert_bulk_data(
        count=args.count,
        batch_size=min(25, args.batch_size),  # Ensure batch size doesn't exceed 25
        max_workers=args.workers
    )


if __name__ == "__main__":
    main()
