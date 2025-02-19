import boto3
import json
from botocore.config import Config

# Configure boto3
config = Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=50
)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1', config=config)
table = dynamodb.Table('mydemo')

# File to store items
ITEMS_FILE = 'dynamodb_items.json'

def scan_and_save_items(target_size=1000000):
    print("Scanning table and saving items...")
    items = []
    scan_kwargs = {
        'ProjectionExpression': 'oneid, #t',
        'ExpressionAttributeNames': {'#t': 'type'},
        'Limit': 1000
    }
    last_evaluated_key = None

    while len(items) < target_size:
        if last_evaluated_key:
            scan_kwargs['ExclusiveStartKey'] = last_evaluated_key

        response = table.scan(**scan_kwargs)
        items.extend([{'oneid': item['oneid'], 'type': item['type']} for item in response['Items']])
        last_evaluated_key = response.get('LastEvaluatedKey')

        if not last_evaluated_key:
            break

    # Remove duplicates while preserving order
    items = list({(item['oneid'], item['type']): item for item in items}.values())
    print(f"Scan complete. Total unique items: {len(items)}")

    # Save items to file
    with open(ITEMS_FILE, 'w') as f:
        json.dump(items, f)

    print(f"Items saved to {ITEMS_FILE}")

if __name__ == "__main__":
    scan_and_save_items()
