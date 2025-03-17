#!/usr/bin/env python3
import boto3
import time
import logging
from boto3.dynamodb.conditions import Key, Attr
from botocore.config import Config
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure boto3 connection
boto_config = Config(
    retries={'max_attempts': 5, 'mode': 'adaptive'},
    connect_timeout=5,
    read_timeout=30
)

class MapDynamoDBQuery:
    def __init__(self, table_name='mapdemo', region='us-east-1', endpoint_url=None):
        """Initialize the DynamoDB query manager"""
        self.table_name = table_name
        self.region = region
        self.endpoint_url = endpoint_url

        # Initialize DynamoDB resources
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=self.region,
            config=boto_config,
            endpoint_url=self.endpoint_url
        )
        self.table = self.dynamodb.Table(self.table_name)

        # Use the existing GSI
        self.gsi_name = 'tile-tsver-index'

    def get_latest_element_versions(self, tile, max_tsver):
        """
        Get the latest version of each element in a tile with tsver <= max_tsver
        using the existing GSI 'tile-tsver-index'
        """
        logger.info(f"Querying latest element versions for tile: {tile} with max_tsver: {max_tsver}")
        start_time = time.time()

        try:
            # Query using the existing GSI
            results = []
            last_evaluated_key = None

            # Paginate through results to handle large datasets
            while True:
                query_params = {
                    'IndexName': self.gsi_name,
                    'KeyConditionExpression': Key('tile').eq(tile) & Key('tsver').lte(max_tsver),
                    'ScanIndexForward': False  # Get items in descending order by tsver
                }

                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key

                response = self.table.query(**query_params)

                items = response.get('Items', [])
                results.extend(items)
                last_evaluated_key = response.get('LastEvaluatedKey')

                logger.info(f"Retrieved batch of {len(items)} items")

                if not last_evaluated_key:
                    break

            # Process results to find the latest version of each element
            # Since we're querying in descending order by tsver, the first occurrence
            # of each element will be its latest version
            latest_versions = {}
            for item in results:
                element = item.get('element')
                tsver = item.get('tsver')

                if element not in latest_versions:
                    latest_versions[element] = {
                        'element': element,
                        'max_tsver': tsver
                    }

            # Convert to list of results
            final_results = list(latest_versions.values())

            elapsed_time = time.time() - start_time
            logger.info(f"Query completed in {elapsed_time:.2f} seconds")
            logger.info(f"Found {len(final_results)} unique elements with their latest versions")

            return final_results, elapsed_time

        except Exception as e:
            logger.error(f"Error querying latest element versions: {e}")
            raise

    def get_latest_element_versions_optimized(self, tile, max_tsver, batch_size=100):
        """
        Optimized version that processes results in batches to reduce memory usage
        for tiles with many elements or versions
        """
        logger.info(f"Querying latest element versions for tile: {tile} with max_tsver: {max_tsver}")
        start_time = time.time()

        try:
            latest_versions = {}
            last_evaluated_key = None
            total_items_processed = 0

            # Process in batches
            while True:
                query_params = {
                    'IndexName': self.gsi_name,
                    'KeyConditionExpression': Key('tile').eq(tile) & Key('tsver').lte(max_tsver),
                    'ScanIndexForward': False,  # Get items in descending order by tsver
                    'Limit': batch_size
                }

                if last_evaluated_key:
                    query_params['ExclusiveStartKey'] = last_evaluated_key

                response = self.table.query(**query_params)

                items = response.get('Items', [])
                total_items_processed += len(items)

                # Process this batch
                for item in items:
                    element = item.get('element')
                    tsver = item.get('tsver')

                    if element not in latest_versions:
                        latest_versions[element] = {
                            'element': element,
                            'max_tsver': tsver
                        }

                last_evaluated_key = response.get('LastEvaluatedKey')

                logger.info(f"Processed batch of {len(items)} items, found {len(latest_versions)} unique elements so far")

                if not last_evaluated_key:
                    break

            # Convert to list of results
            final_results = list(latest_versions.values())

            elapsed_time = time.time() - start_time
            logger.info(f"Query completed in {elapsed_time:.2f} seconds")
            logger.info(f"Processed {total_items_processed} total items")
            logger.info(f"Found {len(final_results)} unique elements with their latest versions")

            return final_results, elapsed_time

        except Exception as e:
            logger.error(f"Error in optimized query for latest element versions: {e}")
            raise

def main():
    # Create query manager
    query_manager = MapDynamoDBQuery(table_name='mapdemo', region='us-east-1')

    # Example query parameters
    tile = 'branch4#t00905'
    max_tsver = 1742218602132

    # Get latest element versions using the optimized method
    start_time = time.time()
    results, query_time = query_manager.get_latest_element_versions_optimized(tile, max_tsver)
    total_time = time.time() - start_time

    # Print results
    print(f"\nLatest versions of elements in tile {tile} with tsver <= {max_tsver}:")
    print(f"Total unique elements: {len(results)}")

    # Print first 10 results as sample
    for i, item in enumerate(results[:10]):
        print(f"{i+1}. Element: {item['element']}, Max TSVer: {item['max_tsver']}")

    if len(results) > 10:
        print(f"... and {len(results) - 10} more elements")
    
    # Print execution time
    print(f"\nExecution time details:")
    print(f"  DynamoDB query time: {query_time:.2f} seconds")
    print(f"  Total execution time: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
