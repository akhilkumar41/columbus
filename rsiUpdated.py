from kiteconnect import KiteConnect
import time
from datetime import datetime, time as dt_time
from tabulate import tabulate
import pandas as pd
import pickle
import json
import os
from tradingview_ta import TA_Handler, Interval
import re
import concurrent.futures
import numpy as np

if not os.path.exists('csv/result'):
    os.makedirs('csv/result')

if not os.path.exists('jsons/result'):
    os.makedirs('jsons/result')


def set_keys():
    apiKey       = 'lstxlmmwrt85d63a'
    apiSecret    = 'kkspf857dt510x856cvjemzx7yaw6e2b'
    tokenFile    = 'access_token.pkl'
     
    with open(tokenFile, 'rb') as f:
            access_token = pickle.load(f)

    accessToken  = access_token
    kite = KiteConnect(api_key=apiKey)
    kite.set_access_token(accessToken)

    print(accessToken)
    return kite



def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def computeRSI(df , period=14):
    #sdf = df.tail(period).copy()

    df['Change'] = df['close'].diff()

    # Calculate gains and losses
    df['Gain'] = np.where(df['Change'] > 0, df['Change'], 0)
    df['Loss'] = np.where(df['Change'] < 0, -df['Change'], 0)

    # Calculate EMA of gains and losses
    df['AvgGain'] = ema(df['Gain'], period)
    df['AvgLoss'] = ema(df['Loss'], period)

    # Calculate RS and RSI
    df['RS'] = df['AvgGain'] / df['AvgLoss']
    df['RSI'] = 100 - (100 / (1 + df['RS']))

    # Return the RSI for the most recent date, rounded to 2 decimal places
    return round(df['RSI'].iloc[-1], 2)


def rsiTradingview(symbol):
        interval = 'Interval.INTERVAL_1_DAY'
        #print(symbol)
        try:
            handler = TA_Handler(
                symbol=symbol,
                screener="india",
                exchange='NSE',
                interval='1d'
            )
            analysis = handler.get_analysis()
            rsi_value = round(analysis.indicators['RSI'] , 2)
            
        except Exception as e:
            rsi_value = 0
        
        return rsi_value


def getRSIUtil(directory):
    files  = os.listdir(directory)

    for file in files:
        file_path = os.path.join(directory, file)
        print(file)

        if file.startswith('.'):
            print("bad file")
            continue
        else:
            current_stock = pd.read_csv(file_path)
            rsi = computeRSI(current_stock)
            base_name = os.path.splitext(file)[0]
            rsi_ta = rsiTradingview(base_name)
            print('diff : ' , round(rsi_ta - rsi , 2))


def getRSI(directory):
    
    daily_directory = directory + "daily"
    weekly_directory = directory + "weekly"
    montly_directory = directory + "montly"
    

    getRSIUtil(daily_directory)




if __name__ == "__main__":
    kite = set_keys()

    directory  = 'csv/historical_data/'

    start_time = time.time()

    getRSI(directory)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total time taken : ", elapsed_time)