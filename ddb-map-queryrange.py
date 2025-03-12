import boto3
from boto3.dynamodb.conditions import Key
import time
import argparse
import json

def query_gsi(block_value='downtown_block_2', max_version=20, table_name='map', 
              index_name='block-version-index', region='us-east-1', limit=100):
    """
    Query a GSI with partition key 'block' and sort key 'version'.
    
    Parameters:
    - block_value: The block to query (e.g., 'downtown_block_2')
    - max_version: Maximum version to consider
    - table_name: DynamoDB table name
    - index_name: GSI name
    - region: AWS region
    - limit: Number of items to display
    
    Returns:
    - Total count of items and the first 'limit' items
    """
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    start_time = time.time()
    print(f"Querying GSI '{index_name}' for block='{block_value}' with version <= {max_version}")
    
    # Convert max_version to both string and number for flexibility
    max_version_str = str(max_version)
    max_version_num = max_version
    
    # We'll try both string and number types for version to handle the schema mismatch
    try:
        # First attempt with version as number
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_num),
            ScanIndexForward=False  # This gives us descending order by version
        )
    except Exception as e:
        print(f"First attempt failed with error: {e}")
        # Second attempt with version as string
        try:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_str),
                ScanIndexForward=False  # This gives us descending order by version
            )
        except Exception as e2:
            print(f"Second attempt failed with error: {e2}")
            # Try a simple query without version condition
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('block').eq(block_value),
                ScanIndexForward=False  # This gives us descending order by version
            )
    
    # Collect all items
    all_items = response.get('Items', [])
    
    # Handle pagination to get all results
    while 'LastEvaluatedKey' in response:
        try:
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_num),
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
        except Exception:
            # Try with string version if number fails
            response = table.query(
                IndexName=index_name,
                KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_str),
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
        
        all_items.extend(response.get('Items', []))
    
    # Filter items where version <= max_version (in case we had to query without version condition)
    filtered_items = []
    for item in all_items:
        item_version = item.get('version')
        # Try to convert version to int if it's stored as string
        if isinstance(item_version, str):
            try:
                item_version = int(item_version)
            except ValueError:
                pass
        
        # Compare with max_version (as int)
        if isinstance(item_version, (int, float)) and item_version <= max_version_num:
            filtered_items.append(item)
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Query completed in {duration:.2f} seconds")
    print(f"Total items found: {len(filtered_items)}")
    
    return filtered_items

def print_items(items, limit=100):
    """
    Print item details in a formatted way
    
    Parameters:
    - items: List of items returned from the query
    - limit: Maximum number of items to print
    """
    if not items:
        print("\n=== NO ITEMS TO DISPLAY ===")
        return
    
    print("\n=== QUERY RESULTS ===")
    
    # Determine the columns to display based on the first item
    if items:
        columns = list(items[0].keys())
        # Move important columns to the front
        for col in ['ele', 'version', 'block']:
            if col in columns:
                columns.remove(col)
                columns.insert(0, col)
    else:
        columns = []
    
    # Print header
    header = " | ".join(f"{col[:15]:<15}" for col in columns)
    print(header)
    print("-" * len(header))
    
    # Print only up to the limit
    for i, item in enumerate(items[:limit]):
        row = " | ".join(f"{str(item.get(col, ''))[:15]:<15}" for col in columns)
        print(row)
    
    if len(items) > limit:
        print(f"\n... and {len(items) - limit} more items (showing {limit} of {len(items)} total)")

def main():
    """Main function to demonstrate the GSI query"""
    parser = argparse.ArgumentParser(description='Query DynamoDB GSI')
    parser.add_argument('--block', type=str, default='downtown_block_2', help='Block value to query')
    parser.add_argument('--max-version', type=int, default=20, help='Maximum version threshold')
    parser.add_argument('--table', type=str, default='map', help='DynamoDB table name')
    parser.add_argument('--index', type=str, default='block-version-index', help='GSI name')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--limit', type=int, default=100, help='Number of items to display')
    
    args = parser.parse_args()
    
    # Execute the query
    items = query_gsi(
        block_value=args.block,
        max_version=args.max_version,
        table_name=args.table,
        index_name=args.index,
        region=args.region,
        limit=args.limit
    )
    
    # Print the results
    print_items(items, limit=args.limit)

if __name__ == "__main__":
    main()
