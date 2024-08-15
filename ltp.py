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


def split_into_chunks(lst, n):
    """Split list into chunks of size n."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def getLTPUtil(kite,tokens):
    final_df = pd.DataFrame()
    chunks = list(split_into_chunks(tokens, 1000))
    for chunk in chunks:
        try:
            ltp_data = kite.ltp(chunk)
            ltp_data = pd.DataFrame(ltp_data).T

            print(len(ltp_data))
            final_df = pd.concat([final_df, ltp_data], ignore_index=True)
        except Exception as e:
            print(f"Unexpected error: {e}")
            continue
    
    final_df = final_df.rename(columns={'last_price': 'ltp'})
    return final_df

def getLTP(kite,tokens):
    result  = getLTPUtil(kite,tokens)
    print(result)
    return result


    
if __name__ == "__main__":
    kite = set_keys()
    tokens = ['3047169' , '123649' , '4882689' , '4532225']
    getLTP(kite,tokens)
    