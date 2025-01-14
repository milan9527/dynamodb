import boto3
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('logdemo')

# Helper functions
def generate_account_address():
    return ''.join(random.choices(string.ascii_letters + string.digits, k=44))

def generate_event_type():
    return random.choice(['SWAP', 'ADD_LIQUIDITY', 'REMOVE_LIQUIDITY'])

def generate_block_time():
    return int(time.time() * 1000000)  # Microseconds

def generate_volume():
    return Decimal(str(random.uniform(-10, 10)))

# Generate a list of large accounts
LARGE_ACCOUNTS = [generate_account_address() for _ in range(5)]  # 5 large accounts

# Write large accounts to a local file
with open('large_accounts.txt', 'w') as f:
    f.write("Large Accounts:\n")
    for idx, account in enumerate(LARGE_ACCOUNTS, 1):
        f.write(f"Large Account {idx}: {account}\n")

print("Large account information has been written to 'large_accounts.txt'")

# Function to create a single item
def create_item(i):
    if i % 2 == 0:  # Every 2nd item will be for a large account
        account_address = LARGE_ACCOUNTS[i % len(LARGE_ACCOUNTS)]
    else:
        account_address = generate_account_address()

    # Random block_number for all accounts within int range
    block_number = random.randint(0, 2**31 - 1)  # 0 to max value for 32-bit signed integer

    return {
        'account_address': account_address,
        'block_number': block_number,
        'event_id': i,
        'date': f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
        'event_type': generate_event_type(),
        'token_account_address': generate_account_address(),
        'token_address': generate_account_address(),
        'opponent_address': generate_account_address(),
        'opponent_token_account_address': generate_account_address(),
        'tx_hash': ''.join(random.choices(string.hexdigits, k=64)),
        'block_time': generate_block_time(),
        'seq': str(random.randint(1, 1000000)),
        'amount': str(random.uniform(0, 1000)),
        'flag': random.randint(0, 1),
        'amm': ''.join(random.choices(string.ascii_lowercase, k=10)),
        'flow_type': random.randint(0, 2),
        'balance_after': str(random.uniform(0, 10000)),
        'token_price_u': str(random.uniform(0, 100)),
        'contract': generate_account_address(),
        'pair_address': generate_account_address(),
        'volume': generate_volume(),
        'extra': ''.join(random.choices(string.ascii_letters, k=100)),
        'is_target': random.choice([True, False]),
        'tx_seq': random.randint(1, 1000),
        'profit': str(random.uniform(-100, 100))
    }

# Function to insert items in batches
def insert_items(start, end):
    with table.batch_writer() as batch:
        for i in range(start, end):
            item = create_item(i)
            batch.put_item(Item=item)
    print(f"Inserted items {start} to {end}")

# Main function to run the insertion
def main():
    total_items = 300000000
    batch_size = 100000
    num_threads = 10

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(0, total_items, batch_size):
            executor.submit(insert_items, i, min(i + batch_size, total_items))

if __name__ == "__main__":
    main()
