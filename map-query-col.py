import boto3
from boto3.dynamodb.conditions import Key
import json
from decimal import Decimal
from datetime import datetime

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def query_latest_item():
    """
    Query the DynamoDB table for the latest item matching the criteria:
    - element_col_id = 'ele636#col2'
    - timestamp <= 1725458208650
    - ordered by timestamp descending
    - limit 1 (get only the most recent item)
    """
    # Initialize DynamoDB resource
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('ElementGeoLocationData')
    
    # Define the query parameters
    element_col_id = 'ele636#col2'
    max_timestamp = 1725458208650
    
    try:
        # Execute the query
        response = table.query(
            KeyConditionExpression=Key('element_col_id').eq(element_col_id) & 
                                  Key('timestamp').lte(max_timestamp),
            ScanIndexForward=False,  # Sort in descending order (newest first)
            Limit=1  # Get only the first (most recent) item
        )
        
        # Check if any items were returned
        items = response.get('Items', [])
        if items:
            # Print the item in a readable format
            print(json.dumps(items[0], indent=2, cls=DecimalEncoder))
            
            # Also show the timestamp as human-readable date
            timestamp_ms = items[0].get('timestamp')
            if timestamp_ms:
                date_time = datetime.fromtimestamp(timestamp_ms / 1000)
                print(f"\nTimestamp {timestamp_ms} as date: {date_time}")
            
            return items[0]
        else:
            print("No items found matching the query criteria.")
            return None
            
    except Exception as e:
        print(f"Error querying DynamoDB: {e}")
        return None

if __name__ == '__main__':
    print("Querying: SELECT * FROM ElementGeoLocationData WHERE element_col_id='ele636#col2' AND timestamp <= 1725458208650 ORDER BY timestamp DESC LIMIT 1")
    query_latest_item()
