from kiteconnect import KiteConnect
import time
from datetime import datetime, time as dt_time
from tabulate import tabulate
import pandas as pd
import pickle
import json
import os
import re
import datetime as dt
from datetime import datetime, timedelta


def set_keys():
    apiKey       = 'lstxlmmwrt85d63a'
    apiSecret    = 'kkspf857dt510x856cvjemzx7yaw6e2b'
    tokenFile    = 'access_token.pkl'
     
    with open(tokenFile, 'rb') as f:
            access_token = pickle.load(f)

    accessToken  = access_token
    kite = KiteConnect(api_key=apiKey)
    kite.set_access_token(accessToken)

    #print(accessToken)
    return kite


def get_historical_data(instrument_token, from_date, to_date, interval):
    try:
        data = []
        while from_date < to_date:
            chunk_to_date = min(to_date, from_date + dt.timedelta(days=2000))
            chunk_data = kite.historical_data(instrument_token, from_date, chunk_to_date, interval)
            data.extend(chunk_data)
            from_date = chunk_to_date + dt.timedelta(days=1)
            time.sleep(1)
        return pd.DataFrame(data)
    except Exception as e:
        #print(f"Error fetching historical data: {e}")
        return None



def fetch_or_update_historical_data(stock):
    from_date = dt.date(2000, 1, 1)
    to_date = datetime.today().date()

    symbol = stock['tradingsymbol']
    instrument_token = stock['instrument_token']
    interval = "day"
    directory = 'csv/historical_data/daily'
        
    file_name = f'{symbol}.csv'
    directory = 'csv/historical_data/daily'
    file_path = os.path.join(directory, file_name)

    if os.path.isfile(file_path):
        #historical data is present 
        current_stock = pd.read_csv(file_path)
        last_updated_str = current_stock.iloc[-1]['date'][:-6]
        last_updated_datetime = datetime.strptime(last_updated_str, "%Y-%m-%d %H:%M:%S")
        last_updated_date = last_updated_datetime.date()  + timedelta(days=1)

        history = get_historical_data(instrument_token, last_updated_date, to_date, interval)
  
        if history is not None:
            current_stock  = pd.concat([current_stock, history], ignore_index=True)
            current_stock.to_csv(f'csv/historical_data/daily/{symbol}.csv', index=False)
            print(f'{symbol} updated')
        
        else:
            print(f'{symbol} not updated missing data last updated ' ,last_updated_date )
        
            
    else:
        #historical data is not present , download fresh file
        history = get_historical_data(instrument_token, from_date, to_date, interval)

        if history is not None:
            history.to_csv(f'csv/historical_data/daily/{symbol}.csv', index=False)
            #print(f'{symbol} saved')
        else:
            print("Failed to retrieve historical data for " , symbol)


def fetch_update_historical_data(symbolTable):
    for index, row in symbolTable.iterrows():
        fetch_or_update_historical_data(row)
        #print('records remaining : ', len(symbolTable) - index)


if __name__ == "__main__":
    kite = set_keys()
    symbolTable_path = 'csv/master/masterSymbolsTable.csv'
    symbolTable  =  pd.read_csv(symbolTable_path)
    
    start_time = time.time()
    fetch_update_historical_data(symbolTable)
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Time taken by function: {elapsed_time:.2f} seconds")
    
