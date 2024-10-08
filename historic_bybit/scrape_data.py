from pybit.unified_trading import HTTP
from datetime import datetime, timedelta
import os
import pandas as pd

from tqdm import tqdm
from utils import get_path, get_data, get_symbols, get_path_csv, extend_data

symbols = get_symbols()

start_time = datetime(2024, 6, 1)
end_time = datetime(2024, 10, 5)

s = start_time.strftime('%Y%m%d-%H:%M:%S')
e = end_time.strftime('%Y%m%d-%H:%M:%S')

directory = 'data'

os.makedirs(directory, exist_ok=True)

for symbol in tqdm(symbols):
    path = get_path_csv(directory, symbol)
    if os.path.exists(path):
        df = pd.read_csv(path)
        df1 = extend_data(df, symbol, end_time)
    else:
        df1 = get_data(symbol, start_time, end_time)
    df1.to_csv(path, index=False)

