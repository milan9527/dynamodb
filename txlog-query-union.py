import boto3
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import time

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('logdemo')

def query_items():
    account_address = "YxwtfP01ogbCZA4yMBg3cKu94A3tZMsKg2GxDOvL3nTA"
    volume_threshold = Decimal('-1.000000')
    limit = 41

    pair_addresses = [
        'uFYoTu1lmIY3cHyx3HBOungXOdNkjF0fFgE5y2AGKeQc',
        'YVl2rDB930a1hYIZQJYRNfHRhQchOCgK6ToQH4EjRPaT',
        'uovm00Av15NPMBLQ4n2Av6fxKj6OS1ZhvgHnu0CRsctc',
        '8xufZUQFgjEFYKc Q9uAvG99rADftBEU9C9vDL3aoUSA3',
        '6xw3ZKeMUcEGgg9mhkFkU6tRoz7GLvnS55eMS4U4W2wY',
        '9Fn4VV5VZvKxEECZK91STY1kASUA9wgD6WGLdonUbK4x',
        'EaZ7HrrYTVc42QzmrvCqvHPjpycJi6A3SFWP1gwa1Rd8'
    ]

    event_types = ['SWAP', 'ADD_LIQUIDITY', 'REMOVE_LIQUIDITY', 'TRANSFER', 'MINT', 'BURN']

    try:
        start_time = time.time()
        items = []

        # First query
        query_params1 = {
            'KeyConditionExpression': Key('account_address').eq(account_address),
            'FilterExpression': Attr('pair_address').is_in(pair_addresses) & 
                                Attr('event_type').is_in(event_types) & 
                                Attr('volume').gte(volume_threshold),
            'ScanIndexForward': False,
            'Limit': limit
        }
        response1 = table.query(**query_params1)
        items.extend(response1['Items'])

        # Second query
        query_params2 = {
            'KeyConditionExpression': Key('account_address').eq(account_address),
            'FilterExpression': Attr('token_address').eq('FhQjKRfcwPkBHuuJzdxbfgQcTnyxuiMi1kxvfTxkpump') & 
                                Attr('event_type').is_in(['TRANSFER', 'MINT', 'BURN']) & 
                                Attr('volume').gte(volume_threshold),
            'ScanIndexForward': False,
            'Limit': limit
        }
        response2 = table.query(**query_params2)
        items.extend(response2['Items'])

        # Sort and limit the combined results
        items.sort(key=lambda x: (-int(x['block_number']), -int(x['event_id'])))
        items = items[:limit]

        end_time = time.time()
        execution_time = end_time - start_time

        return items, execution_time

    except Exception as e:
        print("An error occurred: {}".format(str(e)))
        return [], 0

# Main execution
if __name__ == "__main__":
    items, execution_time = query_items()
    print("Number of items retrieved: {}".format(len(items)))
    print("Execution time: {:.2f} seconds".format(execution_time))
    
    for item in items:
        print("Block Number: {}, Event ID: {}, Event Type: {}, Volume: {}, Amount: {}, Token: {}, Chain: abc, Logo URL: , Symbol: ".format(
            item['block_number'], item['event_id'], item['event_type'], item['volume'], item['amount'], item['token_address']))
        print("Block Time: {}, Date: {}, Token Price U: {}, Tx Hash: {}, Opponent Address: {}, Is Target: {}, Flow Type: {}, Total Fee U: 0, Total Fee: 0".format(
            item['block_time'], item['date'], item.get('token_price_u', ''), item['tx_hash'], item['opponent_address'], item['is_target'], item['flow_type']))
        print("---")
