from kiteconnect import KiteConnect
import time
from datetime import datetime, time as dt_time
import pandas as pd
import pickle
import json
import os
from fuzzywuzzy import fuzz
from tabulate import tabulate
from tradingview_ta import TA_Handler, Interval
import re

if not os.path.exists('csv/historical_data/weekly'):
    os.makedirs('csv/historical_data/weekly')

if not os.path.exists('jsons/historical_data/weekly'):
    os.makedirs('jsons/historical_data/weekly')


if not os.path.exists('csv/historical_data/monthly'):
    os.makedirs('csv/historical_data/monthly')

if not os.path.exists('jsons/historical_data/monthly'):
    os.makedirs('jsons/historical_data/monthly')


def resampler(stock , current_timeframe , target_timeframe):
    
    stock['date'] = pd.to_datetime(stock['date'])
    stock.set_index('date', inplace=True)

    stock = stock.resample(target_timeframe).agg({
    'open': 'first',
    'high': 'max',
    'low': 'min',
    'close': 'last',
    'volume': 'sum'
    })

    return stock


def resample_dailyOHLC_to_weeklyOHLC(directory):
     
     files  = os.listdir(directory)

     for file in files:
        file_path = os.path.join(directory, file)
        #print(file)

        if file.startswith('.'):
            print("bad file")
            continue
        else:
            current_stock = pd.read_csv(file_path)

        #symbol = current_stock['tradingsymbol']
        
        current_timeframe = 'D'
        target_timeframe = 'W-SUN'

        current_stock = resampler(current_stock , current_timeframe , target_timeframe)
        current_stock.index = current_stock.index - pd.to_timedelta(6, unit='d')
        current_stock.reset_index(inplace=True)
        current_stock.to_csv(f'csv/historical_data/weekly/{file}', index=False)


def resample_dailyOHLC_to_montlyOHLC(directory):
    files  = os.listdir(directory)

    for file in files:
        file_path = os.path.join(directory, file)
        #print(file)

        if file.startswith('.'):
            print("bad file")
            continue
        else:
            current_stock = pd.read_csv(file_path)

        #symbol = current_stock['tradingsymbol']
        
        current_timeframe = 'D'
        target_timeframe = 'M'

        current_stock = resampler(current_stock , current_timeframe , target_timeframe)
        current_stock.index = current_stock.index.map(lambda x: x.replace(day=1))
        current_stock.reset_index(inplace=True)
        current_stock.to_csv(f'csv/historical_data/monthly/{file}', index=False)


if __name__ == "__main__":
    directory  = 'csv/historical_data/daily'
    
    start_time = time.time()

    resample_dailyOHLC_to_weeklyOHLC(directory)
    resample_dailyOHLC_to_montlyOHLC(directory)
    
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Time taken by function: {elapsed_time:.2f} seconds")
    
  