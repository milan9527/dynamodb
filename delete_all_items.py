import boto3
import time
import argparse
from botocore.exceptions import ClientError

def delete_all_items(table_name, region='us-east-1'):
    """Delete all items from a DynamoDB table"""
    print(f"Starting deletion of all items from table '{table_name}'...")
    start_time = time.time()
    
    # Initialize DynamoDB resources
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)
    
    # Get the key schema to identify primary key attributes
    try:
        response = boto3.client('dynamodb', region_name=region).describe_table(TableName=table_name)
        key_schema = response['Table']['KeySchema']
        key_attributes = [key['AttributeName'] for key in key_schema]
        print(f"Table primary key attributes: {key_attributes}")
    except ClientError as e:
        print(f"Error getting table schema: {e}")
        return
    
    # Scan the table to get all items
    items = []
    try:
        response = table.scan()
        items.extend(response.get('Items', []))
        
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))
            
        total_items = len(items)
        print(f"Found {total_items} items to delete")
    except ClientError as e:
        print(f"Error scanning table: {e}")
        return
    
    # Delete each item
    deleted_count = 0
    for item in items:
        try:
            # Extract only the key attributes
            key = {attr: item[attr] for attr in key_attributes if attr in item}
            
            # Delete the item
            table.delete_item(Key=key)
            deleted_count += 1
            
            # Print progress every 100 items
            if deleted_count % 100 == 0:
                print(f"Deleted {deleted_count}/{total_items} items...")
                
        except ClientError as e:
            print(f"Error deleting item: {e}")
    
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"Deletion completed in {duration:.2f} seconds")
    print(f"Successfully deleted {deleted_count} of {total_items} items")
    
    return deleted_count

def main():
    """Main function to demonstrate table cleaning"""
    parser = argparse.ArgumentParser(description='Delete all items from a DynamoDB table')
    parser.add_argument('table_name', help='Name of the DynamoDB table to clean')
    parser.add_argument('--region', default='us-east-1', help='AWS region (default: us-east-1)')
    
    args = parser.parse_args()
    
    # Confirm before proceeding
    confirmation = input(f"WARNING: This will delete ALL items from table '{args.table_name}'. Type 'yes' to confirm: ")
    if confirmation.lower() != 'yes':
        print("Operation cancelled.")
        return
        
    # Delete all items
    delete_all_items(args.table_name, args.region)

if __name__ == '__main__':
    main()
