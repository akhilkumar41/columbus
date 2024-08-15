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
from collections import namedtuple

if not os.path.exists('csv/result'):
    os.makedirs('csv/result')

if not os.path.exists('jsons/result'):
    os.makedirs('jsons/result')


def getRSIUtil(symbol , exchange , interval):

    try:
            handler = TA_Handler(
                symbol=symbol,
                screener="india",
                exchange=exchange,
                interval=interval
            )
            analysis = handler.get_analysis()
            rsi_value = round(analysis.indicators['RSI'] , 2)
            

    except Exception as e:
            #print(f"Error fetching RSI for {symbol}: {str(e)}")
            rsi_value = 0
        
    
    return rsi_value


def getRSI(symbol , exchange):
    Result = namedtuple('Result', ['rsi_1d', 'rsi_1w', 'rsi_1m'])

    interval = Interval.INTERVAL_1_DAY
    rsi_1d = getRSIUtil(symbol , exchange , interval)

    interval = Interval.INTERVAL_1_WEEK
    rsi_1w = getRSIUtil(symbol , exchange , interval)

    interval = Interval.INTERVAL_1_MONTH
    rsi_1m = getRSIUtil(symbol , exchange , interval)

    #print(rsi_1d , rsi_1w , rsi_1m)
    return Result(rsi_1d , rsi_1w , rsi_1m)



if __name__ == "__main__":
    getRSI('SYNGENE','NSE')
