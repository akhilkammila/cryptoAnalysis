import pandas as pd
import numpy as np

from pybit.unified_trading import HTTP
from datetime import datetime, timedelta
import os

def get_data(symbol, start_time, end_time):
    session = HTTP(testnet=False)
    all_data = []

    start_time = int(start_time.timestamp() * 1000)
    end_time = int(end_time.timestamp() * 1000)
    
    while start_time <= end_time:
        data = session.get_kline(
            category="linear",
            symbol=symbol,
            interval=60,
            start=start_time,
            end=end_time,
            limit=1000
        )
        ll = data['result']['list']
        if len(ll) == 0:
            break
        last_time = int(ll[-1][0]) - 1000
        end_time = last_time
        all_data.extend(ll)
        df = pd.DataFrame(ll)
    
    col_names = ['time', 'open', 'high', 'low', 'close', 'volume', 'turnover']
    df = pd.DataFrame(all_data, columns=col_names).astype(float)
    df['time'] = pd.to_datetime(df['time'], unit='ms').apply(lambda x: x-timedelta(hours=4))
    return df

def get_symbols():
    session = HTTP(testnet=False)
    http_res =  session.get_tickers(
        category="linear",
    )
    return [res['symbol'] for res in http_res['result']['list']]

def get_path(directory, symbol):
    return os.path.join(directory, symbol + '.pkl')

def get_path_csv(directory, symbol):
    return os.path.join(directory, symbol + '.csv')

def agg_hour(df):
    df_hourly = df.resample('h', on='time').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'turnover': 'sum',
    }).reset_index()
    return df_hourly

def extend_data(df, symbol, end_time):
    start_time = datetime.strptime(df.loc[0,'time'], '%Y-%m-%d %H:%M:%S')
    new_df = get_data(symbol, start_time, end_time)
    combined_df = pd.concat([new_df.iloc[:-1], df], ignore_index=True)
    return combined_df
