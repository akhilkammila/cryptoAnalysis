import asyncio
import os
from datetime import datetime, timedelta
from typing import List, Any

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm_asyncio

# Constants
DIRECTORY = 'data'
BATCH_SIZE = 20  # Number of symbols to process concurrently
dataframes = []
symbs = []

def processDFS(dataframes, symbols):
    # Check if the number of dataframes matches the number of symbols
    if len(dataframes) != len(symbols):
        print(len(dataframes), len(symbols))
        raise ValueError("The number of dataframes must match the number of symbols.")
    
    # Start with the first dataframe
    combined_df = dataframes[0].copy()
    combined_df.columns = ['time', f'open_{symbols[0]}', f'high_{symbols[0]}', 
                           f'low_{symbols[0]}', f'close_{symbols[0]}', 
                           f'volume_{symbols[0]}', f'turnover_{symbols[0]}']
    
    # Merge each subsequent dataframe into the combined_df
    for i in range(1, len(dataframes)):
        df = dataframes[i].copy()
        df.columns = ['time', f'open_{symbols[i]}', f'high_{symbols[i]}', 
                      f'low_{symbols[i]}', f'close_{symbols[i]}', 
                      f'volume_{symbols[i]}', f'turnover_{symbols[i]}']
        
        # Merge on 'time'
        combined_df = pd.merge(combined_df, df, on='time', how='outer')
    
    # Sort by time if needed
    # combined_df.sort_values(by='time', inplace=True)
    return combined_df.reset_index(drop=True)

async def get_data_async(session: aiohttp.ClientSession, symbol: str, start_time: int, end_time: int) -> List[List[Any]]:
    all_data = []
    while start_time <= end_time:
        params = {
            "category": "linear",
            "symbol": symbol,
            "interval": 1,
            "start": start_time,
            "end": end_time,
            "limit": 1000
        }
        async with session.get("https://api.bybit.com/v5/market/kline", params=params) as response:
            data = await response.json()
            ll = data['result']['list']
            if not ll:
                break
            last_time = int(ll[-1][0]) - 1000
            end_time = last_time
            all_data.extend(ll)
    return all_data

async def process_symbol(session: aiohttp.ClientSession, symbol: str, start_time: datetime, end_time: datetime) -> None:
    path = os.path.join(DIRECTORY, f"{symbol}.pq")
    
    if os.path.exists(path):
        df = pd.read_parquet(path)  # Removed parse_dates argument
        df['time'] = pd.to_datetime(df['time'])  # Convert 'time' to datetime
        start_time = df['time'].max() + timedelta(minutes=1)
    
    start_timestamp = int(start_time.timestamp() * 1000)
    end_timestamp = int(end_time.timestamp() * 1000)
    
    all_data = await get_data_async(session, symbol, start_timestamp, end_timestamp)
    
    if all_data:
        col_names = ['time', 'open', 'high', 'low', 'close', 'volume', 'turnover']
        new_df = pd.DataFrame(all_data, columns=col_names).astype(float)
        new_df['time'] = pd.to_datetime(new_df['time'], unit='ms') - timedelta(hours=4)
        
        if os.path.exists(path):
            existing_df = pd.read_parquet(path)  # Removed parse_dates argument
            existing_df['time'] = pd.to_datetime(existing_df['time'])  # Convert 'time' to datetime
            combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['time']).sort_values('time')
        else:
            combined_df = new_df
        
        dataframes.append(combined_df)
        symbs.append(symbol)
        combined_df.to_parquet(path, index=False)

async def get_symbols_async(session: aiohttp.ClientSession) -> List[str]:
    async with session.get("https://api.bybit.com/v5/market/tickers?category=linear") as response:
        data = await response.json()
        return [res['symbol'] for res in data['result']['list']]

async def main():
    os.makedirs(DIRECTORY, exist_ok=True)
    
    start_time = datetime(2024, 6, 1, 0, 0)
    end_time = datetime(2024, 9, 2, 0, 0)
    
    async with aiohttp.ClientSession() as session:
        symbols = await get_symbols_async(session)
        # symbs = symbols
        # print(symbols[:200])
        tasks = []
        for i in range(0, len(symbols), BATCH_SIZE):
            batch = symbols[i:i+BATCH_SIZE]
            batch_tasks = [process_symbol(session, symbol, start_time, end_time) for symbol in batch]
            tasks.extend(batch_tasks)
        
        await tqdm_asyncio.gather(*tasks, desc="Processing symbols")

    combined_df = processDFS(dataframes, symbs)
    # print(combined_df)
    combined_df.to_csv('crypto_data.csv')

if __name__ == "__main__":
    asyncio.run(main())