import boto3
from boto3.dynamodb.conditions import Key
import time
import argparse

def query_max_versions_by_block(block_value, max_version_threshold='20', table_name='map', region='us-east-1'):
    """
    Query elements in a specific block with versions <= threshold,
    returning the max version for each element.
    
    Retrieves items in descending order by version and stops processing
    once we detect a version below our threshold of interest.
    
    Parameters:
    - block_value: The block to query (e.g., 'downtown_block_2')
    - max_version_threshold: Maximum version to consider
    - table_name: DynamoDB table name
    - region: AWS region
    
    Returns:
    - List of items with max version for each element
    """
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    start_time = time.time()
    print(f"Querying for block='{block_value}' with version <= {max_version_threshold}")
    
    # Track elements we've seen - the first occurrence will be the max version
    seen_elements = set()
    max_version_items = []
    items_processed = 0
    
    # Query the GSI with block as partition key and version as sort key
    # ScanIndexForward=False sorts by version in descending order
    response = table.query(
        IndexName='block-version-index',
        KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_threshold),
        ScanIndexForward=False  # This gives us descending order by version
    )
    
    # Keep track of the highest version we've seen
    highest_version_seen = None
    
    # Process initial query results
    for item in response.get('Items', []):
        items_processed += 1
        ele_id = item['ele']
        current_version = item['version']
        
        # Update the highest version we've seen
        if highest_version_seen is None:
            highest_version_seen = current_version
        
        # If this version is lower than the highest we've seen, we can stop
        # processing for this element as we've already found its max
        if ele_id not in seen_elements:
            seen_elements.add(ele_id)
            max_version_items.append(item)
    
    # Handle pagination if needed
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='block-version-index',
            KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_threshold),
            ScanIndexForward=False,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        
        # Continue processing items
        for item in response.get('Items', []):
            items_processed += 1
            ele_id = item['ele']
            current_version = item['version']
            
            # If we see a version lower than the highest, we can stop for this element
            if ele_id not in seen_elements:
                seen_elements.add(ele_id)
                max_version_items.append(item)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Query completed in {duration:.2f} seconds")
    print(f"Processed {items_processed} total items")
    print(f"Found {len(max_version_items)} unique elements with their max versions")
    
    return max_version_items

def print_query_results(results, limit=10):
    """
    Print the query results in a formatted way
    
    Parameters:
    - results: List of items returned from the query
    - limit: Maximum number of results to print
    """
    print("\n=== QUERY RESULTS ===")
    print(f"{'Element ID':<20} {'Version':<10} {'Block':<20}")
    print("-" * 60)
    
    # Print only up to the limit
    for i, item in enumerate(results[:limit]):
        print(f"{item['ele']:<20} {item['version']:<10} {item['block']:<20}")
    
    if len(results) > limit:
        print(f"\n... and {len(results) - limit} more results (showing {limit} of {len(results)} total)")

def main():
    """Main function to demonstrate the query"""
    parser = argparse.ArgumentParser(description='Query max versions by block from DynamoDB')
    parser.add_argument('--block', type=str, default='downtown_block_2', help='Block value to query')
    parser.add_argument('--max-version', type=str, default='20', help='Maximum version threshold')
    parser.add_argument('--table', type=str, default='map', help='DynamoDB table name')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--limit', type=int, default=10, help='Number of results to display')
    
    args = parser.parse_args()
    
    # Execute the query
    results = query_max_versions_by_block(
        block_value=args.block,
        max_version_threshold=args.max_version,
        table_name=args.table,
        region=args.region
    )
    
    # Print the results
    print_query_results(results, limit=args.limit)

if __name__ == "__main__":
    main()
