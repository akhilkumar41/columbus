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
from rsi import getRSI
from ath import getATH
from ltp import getLTP


if not os.path.exists('csv/result/'):
    os.makedirs('csv/result/')

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


def populateRSIUtil(path):
    output_df = pd.read_csv(path)
    output_df['rsi_1d'] = None
    output_df['rsi_1w'] = None
    output_df['rsi_1m'] = None

    #output_df = output_df.head()

    for index, row in output_df.iterrows():
        symbol = row['tradingsymbol']
        exchange = row['exchange']
        #print(symbol , exchange)
        result = getRSI(symbol,exchange)

        output_df.loc[index,'rsi_1d'] = result.rsi_1d
        output_df.loc[index,'rsi_1w'] = result.rsi_1w
        output_df.loc[index,'rsi_1m'] = result.rsi_1m

        print('RSI : ' , symbol , result.rsi_1d)
    
    return output_df[['instrument_token','rsi_1d', 'rsi_1w', 'rsi_1m']]
    

def populateATHUtil(path):
    output_df = pd.read_csv(path)
    output_df['ath'] = None
    output_df['since_ath'] = None

    #output_df = output_df.head()

    for index, row in output_df.iterrows():
        symbol = row['tradingsymbol']
        exchange = row['exchange']

        result = getATH(symbol,exchange)
        output_df.loc[index,'ath'] = result.ath
        output_df.loc[index,'since_ath'] = result.since_ath
        print('ATH:' , result.ath , result.since_ath )
    
    return output_df[['instrument_token','ath', 'since_ath']]


def populateLTPUtil(path):
    output_df = pd.read_csv(path)
    output_df['ltp'] = None
    
    #print(len(output_df))
    token = output_df['instrument_token']
    output_df = getLTP(kite,token)
    return output_df


def mergeOutputandSave(input_0, input_1,input_2,input_3, directory = 'csv/result'):    
    output = pd.concat([input_0, input_1 , input_2 , input_3], axis=1)

    filename = 'outputTable.csv'
    saveTocsv(output , filename)
 

def saveTocsv(df ,filename,directory = 'csv/result'):
    file_path = os.path.join(directory, filename)
    df.to_csv(file_path, index=False)


def joinLtpoutput(list , ltp):
    columns = ['instrument_token' , 'ltp']
    out = pd.DataFrame(columns=columns)

    for index, row in list.iterrows():
        instrument_token = row['instrument_token']
        key_exists = instrument_token in ltp['instrument_token'].values
        
        if key_exists:
            price = ltp.loc[ltp['instrument_token'] == instrument_token, 'ltp'].values[0]
        else:
            price = 0.0
        
        out.loc[index,'instrument_token'] = instrument_token
        out.loc[index,'ltp'] = price

    return out


def populateOutput(directory):
    #filename = 'masterSymbolsTable.csv'
    filename = 'masterNFOTable.csv'
    file_path = os.path.join(directory, filename)
    main_file = pd.read_csv(file_path)

    start_time = time.time()
    rsi_output = populateRSIUtil(file_path)
    saveTocsv(rsi_output , 'rsiOutput.csv')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total time taken by rsi.py : ", round(elapsed_time,2))
    
    start_time = time.time()
    ath_output = populateATHUtil(file_path)
    saveTocsv(ath_output , 'athOutput.csv')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total time taken by ath.py : ", round(elapsed_time,2))

    start_time = time.time()
    ltp_output = populateLTPUtil(file_path)
    ltp_output = joinLtpoutput(main_file , ltp_output)
    saveTocsv(ltp_output , 'ltpOutput.csv')
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total time taken by ltp.py : ", round(elapsed_time,2))

    rsi_output = rsi_output.drop('instrument_token', axis=1)
    ath_output = ath_output.drop('instrument_token', axis=1)
    ltp_output = ltp_output.drop('instrument_token', axis=1)

    print(len(rsi_output) , len(ath_output) , len(ltp_output))
    mergeOutputandSave(main_file, rsi_output , ath_output , ltp_output)

if __name__ == "__main__":
    kite = set_keys()
    directory = 'csv/master/'
    start_time = time.time()

    populateOutput(directory)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("Total time taken by ath and rsi: ", round(elapsed_time,2))