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
import datetime as dt
from collections import namedtuple


def getATHUtil(path):
    Result = namedtuple('Result', ['ath', 'since_ath'])
    
    if os.path.isfile(path):
        ath_df = pd.read_csv(path)
        ath = ath_df['high'].max()
        #ath_at_index = ath_df['high'].idxmax()
        ath_at_index = ath_df['high'].values.argmax()
        return Result(ath , len(ath_df) - ath_at_index)
    
    print('File not found for :',path)
    return Result(0,0)



def getATH(symbol , exchange):
    path = f'csv/historical_data/daily/{symbol}.csv'
    result  = getATHUtil(path)

    return result


    
if __name__ == "__main__":
    getATH('SYNGENE','NSE')

    