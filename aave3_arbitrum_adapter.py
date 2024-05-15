import pandas as pd
import numpy as np
import datetime as dt
import requests
from collections import defaultdict
from dotenv import load_dotenv
import os
from subgrounds import Subgrounds

# Initialize Subgraph
load_dotenv()
api_key = os.getenv('THEGRAPH_API')
sg = Subgrounds()
v3_pool = sg.load_subgraph(f"https://gateway-arbitrum.network.thegraph.com/api/{api_key}/subgraphs/id/2kmbjLfKbEDz2bKxrg7FtcnBg84ghBHkGrLXENbDBgEv")

# Getting the first block and the last block of the previous hour
def get_block_range_previous_hour():
    current_time = dt.datetime.now()
    previous_hour_time = current_time - dt.timedelta(hours=1)
    
    # Get the first block of the previous hour
    first_block_query = v3_pool.Query.supplies(
        first=1,
        orderBy=v3_pool.Supply.blockTimestamp,
        orderDirection='asc',
        where={
            'blockTimestamp_gte': int(previous_hour_time.timestamp())
        }
    )
    first_block_df = sg.query_df([first_block_query.blockNumber, first_block_query.blockTimestamp])
    
    if first_block_df.empty:
        raise ValueError("No blocks found for the start of the previous hour.")
    
    first_block_number = first_block_df['supplies_blockNumber'][0]
    first_block_timestamp = first_block_df['supplies_blockTimestamp'][0]
    
    # Get the last block of the previous hour
    last_block_query = v3_pool.Query.supplies(
        first=1,
        orderBy=v3_pool.Supply.blockTimestamp,
        orderDirection='desc',
        where={
            'blockTimestamp_lte': int(current_time.timestamp())
        }
    )
    last_block_df = sg.query_df([last_block_query.blockNumber, last_block_query.blockTimestamp])
    
    if last_block_df.empty:
        raise ValueError("No blocks found for the end of the previous hour.")
    
    last_block_number = last_block_df['supplies_blockNumber'][0]
    last_block_timestamp = last_block_df['supplies_blockTimestamp'][0]
    
    return int(first_block_number), int(last_block_number), int(first_block_timestamp), int(last_block_timestamp)


# The relation token decimals -- Address is hardcoded but we can improve later for scalability purposes :)
token_decimals = {
    '0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8'.lower(): ['USDC',6],  # USDC
    '0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'.lower(): ['USDT',6],  # USDT
    '0x82aF49447D8a07e3bd95BD0d56f35241523fBab1'.lower(): ['ETH',18], # WETH
}

# Adjust amount based on token decimals
def adjust_amount(token_address, amount):
    decimals = token_decimals[token_address][1]
    return amount / (10 ** decimals)


# Function to query and process events for a specific block range
def process_block_range(start_block, last_block):
    supplies_query = v3_pool.Query.supplies(
        where={
            'blockNumber_gte': start_block,
            'blockNumber_lte': last_block,
            'reserve_in': list(token_decimals.keys())
        }
    )
    withdraws_query = v3_pool.Query.withdraws(
        where={
            'blockNumber_gte': start_block,
            'reserve_in': list(token_decimals.keys())
        }
    )
    borrows_query = v3_pool.Query.borrows(
        where={
            'blockNumber_gte': start_block,
            'blockNumber_lte': last_block,
            'reserve_in': list(token_decimals.keys())
        }
    )
    repays_query = v3_pool.Query.repays(
        where={
            'blockNumber_gte': start_block,
            'blockNumber_lte': last_block,
            'reserve_in': list(token_decimals.keys())
        }
    )
    liquidationCalls_query = v3_pool.Query.liquidationCalls(
        where={
            'blockNumber_gte': start_block,
                'blockNumber_lte': last_block,
            'collateralAsset_in': list(token_decimals.keys())
        }
    )

    # Fetch data
    supplies_df = sg.query_df([supplies_query.id, supplies_query.reserve, supplies_query.user, supplies_query.amount, supplies_query.blockNumber, supplies_query.blockTimestamp])
    withdraws_df = sg.query_df([withdraws_query.id, withdraws_query.reserve, withdraws_query.user, withdraws_query.amount, withdraws_query.blockNumber, withdraws_query.blockTimestamp])
    borrows_df = sg.query_df([borrows_query.id, borrows_query.reserve, borrows_query.user, borrows_query.amount, borrows_query.blockNumber, borrows_query.blockTimestamp])
    repays_df = sg.query_df([repays_query.id, repays_query.reserve, repays_query.user, repays_query.amount, repays_query.blockNumber, repays_query.blockTimestamp])
    liquidationCalls_df = sg.query_df([liquidationCalls_query.id, liquidationCalls_query.collateralAsset, liquidationCalls_query.debtAsset, liquidationCalls_query.user, liquidationCalls_query.debtToCover, liquidationCalls_query.liquidatedCollateralAmount, liquidationCalls_query.blockNumber, liquidationCalls_query.blockTimestamp])

    # Process data
    user_balances = {}

    # Process supplies
    for _, row in supplies_df.iterrows():
        user = row['supplies_user']
        token = row['supplies_reserve']
        adjusted_amount = adjust_amount(token, row['supplies_amount'])

        if user not in user_balances:
            user_balances[user] = {}
        if token not in user_balances[user]:
            user_balances[user][token] = 0

        user_balances[user][token] += adjusted_amount

    # Process withdraws
    for _, row in withdraws_df.iterrows():
        user = row['withdraws_user']
        token = row['withdraws_reserve']
        adjusted_amount = adjust_amount(token, row['withdraws_amount'])

        if user not in user_balances:
            user_balances[user] = {}
        if token not in user_balances[user]:
            user_balances[user][token] = 0

        user_balances[user][token] -= adjusted_amount

    # Process borrows
    for _, row in borrows_df.iterrows():
        user = row['borrows_user']
        token = row['borrows_reserve']
        adjusted_amount = adjust_amount(token, row['borrows_amount'])

        if user not in user_balances:
            user_balances[user] = {}
        if token not in user_balances[user]:
            user_balances[user][token] = 0

        user_balances[user][token] -= adjusted_amount

    # Process repays
    for _, row in repays_df.iterrows():
        user = row['repays_user']
        token = row['repays_reserve']
        adjusted_amount = adjust_amount(token, row['repays_amount'])

        if user not in user_balances:
            user_balances[user] = {}
        if token not in user_balances[user]:
            user_balances[user][token] = 0

        user_balances[user][token] += adjusted_amount

    # Process liquidation calls
    for _, row in liquidationCalls_df.iterrows():
        user = row['liquidationCalls_user']
        token = row['liquidationCalls_collateralAsset']
        adjusted_amount = adjust_amount(token, row['liquidationCalls_liquidatedCollateralAmount'])

        if user not in user_balances:
            user_balances[user] = {}
        if token not in user_balances[user]:
            user_balances[user][token] = 0

        user_balances[user][token] -= adjusted_amount
    return user_balances

# Convert balances to DataFrame
def balances_to_dataframe(user_balances, block_number, block_timestamp):
    records = []
    for user, tokens in user_balances.items():
        for token, amount in tokens.items():
            records.append({
                'block_number': block_number,
                'timestamp': dt.datetime.fromtimestamp(block_timestamp),
                'owner_address': user,
                'token_address': token,
                'token_symbol': token_decimals[token][0],
                'token_amount': float(amount)
            })
    return pd.DataFrame(records)



if __name__ == "__main__":
    # Get the last block of the previous hour
    start_block, last_block, first_block_timestamp, last_block_timestamp = get_block_range_previous_hour() # Assign variables
    
    # Process the events for this block
    user_balances = process_block_range(start_block,last_block)
    
    # Convert to DataFrame
    df_balances = balances_to_dataframe(user_balances, last_block, last_block_timestamp)

    # Save the final DataFrame to a CSV file
    df_balances.to_csv('net_supplied_amount_previous_hour.csv', index=False)