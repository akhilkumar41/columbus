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




if __name__ == "__main__":
    kite = set_keys()
    
    file_path = 'csv/result/outputTable.csv'
    table = pd.read_csv(file_path)

    table["delta"] = ((table["ltp"] - table["ath"])/table["ath"])*100
    table["delta"] = round(table["delta"] , 2)

    selected_df = table[(table['rsi_1m'] >=60 ) & (table['rsi_1w'] >= 60 ) & (table['rsi_1d'] < 50 )]

    column = ['instrument_token','exchange_token' , 'name' , 'sector' , 'industry']
    selected_df = selected_df.drop(column , axis=1)

    print(tabulate(selected_df, headers='keys', tablefmt='grid'))

