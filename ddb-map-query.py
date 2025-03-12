import boto3
from boto3.dynamodb.conditions import Key
import time
import argparse

def query_max_versions_by_block(block_value, max_version_threshold=20, table_name='map', region='us-east-1'):
    """
    Query elements in a specific block with versions <= threshold,
    stopping as soon as we encounter a version less than the threshold.
    
    Parameters:
    - block_value: The block to query (e.g., 'downtown_block_2')
    - max_version_threshold: Maximum version to consider
    - table_name: DynamoDB table name
    - region: AWS region
    
    Returns:
    - List of items with versions equal to the threshold
    """
    # Initialize DynamoDB client
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    start_time = time.time()
    print(f"Querying for block='{block_value}' with version <= {max_version_threshold}")
    
    # Convert max_version_threshold to string for DynamoDB query
    max_version_str = str(max_version_threshold)
    max_version_int = int(max_version_threshold)
    
    # Track elements we've seen
    seen_elements = set()
    result_items = []
    items_processed = 0
    
    # Query the GSI with block as partition key and version as sort key
    # ScanIndexForward=False sorts by version in descending order
    response = table.query(
        IndexName='block-version-index',
        KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_str),
        ScanIndexForward=False  # This gives us descending order by version
    )
    
    # Process initial query results
    for item in response.get('Items', []):
        items_processed += 1
        ele_id = item['ele']
        current_version = int(item['version']) if isinstance(item['version'], str) else item['version']
        
        # Stop processing if we encounter a version less than max_version
        if current_version < max_version_int:
            print(f"Early termination: Found version {current_version} < {max_version_int}")
            break
        
        # If we haven't seen this element before, add it to results
        if ele_id not in seen_elements:
            seen_elements.add(ele_id)
            result_items.append(item)
    
    # Handle pagination if needed, with early termination check
    while 'LastEvaluatedKey' in response and items_processed > 0:
        response = table.query(
            IndexName='block-version-index',
            KeyConditionExpression=Key('block').eq(block_value) & Key('version').lte(max_version_str),
            ScanIndexForward=False,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        
        # Early termination flag
        should_break = False
        
        # Continue processing items
        for item in response.get('Items', []):
            items_processed += 1
            ele_id = item['ele']
            current_version = int(item['version']) if isinstance(item['version'], str) else item['version']
            
            # Stop processing if we encounter a version less than max_version
            if current_version < max_version_int:
                print(f"Early termination: Found version {current_version} < {max_version_int}")
                should_break = True
                break
                
            # If we haven't seen this element before, add it to results
            if ele_id not in seen_elements:
                seen_elements.add(ele_id)
                result_items.append(item)
        
        # If we broke out of the inner loop due to early termination, also break out of the outer loop
        if should_break:
            break
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Query completed in {duration:.2f} seconds")
    print(f"Processed {items_processed} total items")
    print(f"Found {len(result_items)} unique elements with version {max_version_int}")
    
    return result_items

def print_query_results(results, limit=10):
    """
    Print the query results in a formatted way
    
    Parameters:
    - results: List of items returned from the query
    - limit: Maximum number of results to print
    """
    if not results:
        print("\n=== NO RESULTS TO DISPLAY ===")
        return
        
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
    parser.add_argument('--max-version', type=int, default=20, help='Maximum version threshold')
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
