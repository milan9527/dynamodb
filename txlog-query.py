import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import time

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('logdemo')

def query_items():
    account_address = "YxwtfP01ogbCZA4yMBg3cKu94A3tZMsKg2GxDOvL3nTA"
    event_types = ['SWAP', 'ADD_LIQUIDITY']
    volume_threshold = Decimal('-1.000000')
    limit = 100

    try:
        start_time = time.time()
        items = []

        response = table.query(
            IndexName='volume-index',
            KeyConditionExpression=Key('account_address').eq(account_address) & Key('volume').gte(volume_threshold),
            FilterExpression=Attr('event_type').is_in(event_types),
            ScanIndexForward=False  # This will sort volume in descending order
        )

        items.extend(response['Items'])

        # Continue querying if there are more results and we haven't reached 100 items
        while 'LastEvaluatedKey' in response and len(items) < limit:
            response = table.query(
                IndexName='volume-index',
                KeyConditionExpression=Key('account_address').eq(account_address) & Key('volume').gte(volume_threshold),
                FilterExpression=Attr('event_type').is_in(event_types),
                ScanIndexForward=False,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])

        # Sort the items by block_number and event_id in descending order
        sorted_items = sorted(items, 
                              key=lambda x: (int(x['block_number']), x['event_id']), 
                              reverse=True)

        # Limit to 100 items after sorting
        final_items = sorted_items[:limit]

        end_time = time.time()
        execution_time = end_time - start_time

        return final_items, execution_time

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return [], 0

# Main execution
if __name__ == "__main__":
    items, execution_time = query_items()
    print(f"Number of items retrieved: {len(items)}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    for item in items:
        print(item)
