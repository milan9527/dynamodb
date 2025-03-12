import boto3
from boto3.dynamodb.conditions import Key, Attr
import time

def query_max_versions_by_block(block_value, max_version_threshold='20', table_name='map', region='us-east-1'):
    """
    Query elements in a specific block with versions <= threshold,
    returning the max version for each element using DynamoDB API features.
    
    Parameters:
    - block_value: The block to query (e.g., 'downtown_block_2')
    - max_version_threshold: Maximum version to consider (as string)
    - table_name: DynamoDB table name
    - region: AWS region
    
    Returns:
    - List of dictionaries containing element IDs and their max versions
    """
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    start_time = time.time()
    print(f"Querying for block='{block_value}' with version <= {max_version_threshold}")
    
    # Use DynamoDB query with ScanIndexForward=False to get items in descending order by version
    # This leverages the block-version-index GSI
    response = table.query(
        IndexName='block-version-index',
        KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_threshold),
        ScanIndexForward=False  # This sorts by sort key (version) in descending order
    )
    
    # Process the results - get the first occurrence of each element (which will be its max version)
    elements = {}
    items_processed = 0
    
    # Process initial query results
    for item in response.get('Items', []):
        items_processed += 1
        ele_id = item['ele']
        # Only keep the first occurrence of each element (which is the max version due to sorting)
        if ele_id not in elements:
            elements[ele_id] = item
    
    # Handle pagination if needed
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName='block-version-index',
            KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_threshold),
            ScanIndexForward=False,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        
        for item in response.get('Items', []):
            items_processed += 1
            ele_id = item['ele']
            # Only keep the first occurrence of each element
            if ele_id not in elements:
                elements[ele_id] = item
    
    # Convert to list
    result = list(elements.values())
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Query completed in {duration:.2f} seconds")
    print(f"Processed {items_processed} total items")
    print(f"Found {len(result)} unique elements with their max versions")
    
    return result

def print_query_results(results, limit=10):
    """
    Print the query results in a formatted way
    
    Parameters:
    - results: List of items returned from the query
    - limit: Maximum number of results to print
    """
    print("\n=== QUERY RESULTS ===")
    print(f"{'Element ID':<40} {'Version':<10} {'Status':<20} {'Timestamp'}")
    print("-" * 80)
    
    # Print only up to the limit
    for i, item in enumerate(results[:limit]):
        print(f"{item['ele']:<40} {item['version']:<10} {item.get('status', 'N/A'):<20} {item.get('timestamp', 'N/A')}")
    
    if len(results) > limit:
        print(f"\n... and {len(results) - limit} more results (showing {limit} of {len(results)} total)")

def main():
    """Main function to demonstrate the query"""
    # Parse command line arguments
    import argparse
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
