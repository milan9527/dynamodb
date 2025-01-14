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
        last_evaluated_key = None

        while len(items) < limit:
            # Query parameters
            query_params = {
                'KeyConditionExpression': Key('account_address').eq(account_address),
                'FilterExpression': Attr('event_type').is_in(event_types) & Attr('volume').gte(volume_threshold),
                'ScanIndexForward': False,  # This will sort block_number in descending order
                'Limit': limit  # This limits the items per query, not the final result
            }

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            response = table.query(**query_params)
            items.extend(response['Items'])

            # If there are no more items to query, break the loop
            if 'LastEvaluatedKey' not in response:
                break

            last_evaluated_key = response['LastEvaluatedKey']

            # Break if we have enough items
            if len(items) >= limit:
                break

        # Sort items by block_number (desc) and then by event_id (desc)
        items.sort(key=lambda x: (-int(x['block_number']), -int(x['event_id'])))

        # Ensure we only return 'limit' number of items
        items = items[:limit]

        end_time = time.time()
        execution_time = end_time - start_time

        return items, execution_time

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return [], 0

# Main execution
if __name__ == "__main__":
    items, execution_time = query_items()
    print(f"Number of items retrieved: {len(items)}")
    print(f"Execution time: {execution_time:.2f} seconds")
    
    for item in items:
        print(f"Block Number: {item['block_number']}, Event ID: {item['event_id']}, Event Type: {item['event_type']}, Volume: {item['volume']}")
