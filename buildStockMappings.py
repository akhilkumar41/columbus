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

if not os.path.exists('csv'):
    os.makedirs('csv')

if not os.path.exists('jsons'):
    os.makedirs('jsons')

if not os.path.exists('csv/mapped_sector_industry'):
    os.makedirs('csv/mapped_sector_industry')

if not os.path.exists('jsons/mapped_sector_industry'):
    os.makedirs('jsons/mapped_sector_industry')


if not os.path.exists('jsons/master'):
    os.makedirs('jsons/master')


if not os.path.exists('csv/master'):
    os.makedirs('csv/master')


if not os.path.exists('jsons/input'):
    os.makedirs('jsons/input')


if not os.path.exists('csv/input'):
    os.makedirs('csv/input')


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

def get_instruments_kiteconnect():
     instruments = kite.instruments("NSE")
     jsonFile    = 'jsons/input/allInstruments_kite.json'
     with open(jsonFile, 'w') as json_file:
            json.dump(instruments, json_file, indent=4)
     
     instruments = pd.DataFrame(instruments)
     instruments.to_csv('csv/input/allInstruments_kite.csv')

def store_files():
    #https://stocksonfire.in/trading-ideas/nse-stocks-sector-wise-sorting-excel-sheet/ , to get this csv
    fileStockSector = pd.read_csv('csv/input/India_Sector_Industry_2024_MAY.csv')
    fileStockSector_NSE = fileStockSector[fileStockSector['Exchange'].str.contains('NSE')]
    fileStockSector_BSE = fileStockSector[fileStockSector['Exchange'].str.contains('BSE')]

    fileStockSector_NSE.to_csv('csv/input/India_Sector_Industry_2024_MAY_NSE.csv', index=False)
    fileStockSector_BSE.to_csv('csv/input/India_Sector_Industry_2024_MAY_BSE.csv', index=False)
    #print(fileStockSector.columns)
    #kiteconnect ==> ['Description', 'Sector', 'Industry', 'Exchange']

    #store in json format as well 
    fileStockSector     = fileStockSector.to_dict(orient='records')
    fileName            = 'jsons/input/India_Sector_Industry_2024_MAY.json' 
    with open(fileName, 'w') as json_file:
         json.dump(fileStockSector, json_file, indent=4)

    fileStockSector_NSE = fileStockSector_NSE.to_dict(orient='records')
    fileName            = 'jsons/input/India_Sector_Industry_2024_MAY_NSE.json' 
    with open(fileName, 'w') as json_file:
         json.dump(fileStockSector_NSE, json_file, indent=4)

    
    fileStockSector_BSE = fileStockSector_BSE.to_dict(orient='records')
    fileName            = 'jsons/input/India_Sector_Industry_2024_MAY_BSE.json' 
    with open(fileName, 'w') as json_file:
         json.dump(fileStockSector_BSE, json_file, indent=4)


def map_stock_to_sector():
    #make sure below file is in input folder:
    nseStockDescription = pd.read_csv('csv/input/India_Sector_Industry_2024_MAY_NSE.csv')
    nseStockSymbol  = pd.read_csv('csv/input/allInstruments_kite.csv')
    
    columns = ['instrument_token' , 'exchange_token' , 'tradingsymbol' , 'name' , 'sector' , 'industry' , 'exchange']
    mappedStockSymbol = pd.DataFrame(columns=columns)
    

    count = 0 
    #sector loop
    for index, row in nseStockDescription.iterrows():
        to_search_description = row['Description']
        
        if pd.isna(to_search_description) == True:
             continue
        
        max_score = 0
        final_index = 0
        min_score = 70
        #symbol loop kite
        for index_a , row_a in nseStockSymbol.iterrows():
            current_description = row_a['name']
            
            if pd.isna(current_description) == False:
                score = fuzz.ratio(to_search_description, current_description)
                if score > max_score and score > min_score:
                    max_score = score
                    final_index = index_a

        if max_score > min_score :
            new_row={
                        'instrument_token': nseStockSymbol['instrument_token'][final_index], 
                        'exchange_token':   nseStockSymbol['exchange_token'][final_index] , 
                        'tradingsymbol':    nseStockSymbol['tradingsymbol'][final_index] , 
                        'name' :            nseStockSymbol['name'][final_index] , 
                        'sector' :          row['Sector'] , 
                        'industry' :        row['Industry'] ,
                        'exchange' :        nseStockSymbol['exchange'][final_index] 
                    }
        #print(new_row)

            print(count , "score = " , max_score , " , " , to_search_description , " , " , nseStockSymbol['name'][final_index])  
            count = count + 1
            mappedStockSymbol.loc[len(mappedStockSymbol)] = new_row
    
    mappedStockSymbol = mappedStockSymbol.drop_duplicates()
    mappedStockSymbol.to_csv('csv/master/mapped_stock_sector.csv', index=False)
    
    mappedStockSymbol_df = mappedStockSymbol.to_dict(orient='records')
    fileName            = 'jsons/master/mapped_stock_sector.json' 
    with open(fileName, 'w') as json_file:
         json.dump(mappedStockSymbol_df, json_file, indent=4)
    


def seggregate_stocks_by_sector_and_Industry():
    mappedStockToSector = pd.read_csv('csv/master/mapped_stock_sector.csv')
    unique_industries_by_sector = mappedStockToSector.groupby('sector')['industry'].unique()
    
    final_df = pd.DataFrame()
    #print(unique_industries_by_sector)
    for sector, industries in unique_industries_by_sector.items():
        words = re.split(r'[^a-zA-Z0-9]', sector)
        words = [word for word in words if word]    
        transformed_sector = ''.join(word.capitalize() for word in words)
        #print(f"Sector: {transformed_sector}")

        for industry in industries:
            #print(f" - {industry}")
            stocks = mappedStockToSector[(mappedStockToSector['sector'] == sector) & (mappedStockToSector['industry'] == industry)]

            words = re.split(r'[^a-zA-Z0-9]', industry)
            words = [word for word in words if word]
            transformed_industry = ''.join(word.capitalize() for word in words)
            
            filename = f'csv/mapped_sector_industry/{transformed_sector}_{transformed_industry}_sector_industry_mapping.csv'
            stocks.to_csv(filename, index=False)

            final_df  = pd.concat([final_df, stocks], ignore_index=True)

            json_df = stocks.to_dict(orient='records')
            filename_json  = f'jsons/mapped_sector_industry/{transformed_sector}_{transformed_industry}_sector_industry_mapping.json' 
            with open(filename_json, 'w') as json_file:
                json.dump(json_df, json_file, indent=4)
        
    
    final_df.to_csv('csv/master/masterSymbolsTable.csv', index=False)
    json_final_df = final_df.to_dict(orient='records')
    filename_json = 'jsons/master/masterSymbolsTable.json'
    with open(filename_json, 'w') as json_file:
                json.dump(json_final_df, json_file, indent=4)




def get_NFO_stocks():
     full_table_df = pd.read_csv('csv/master/masterSymbolsTable.csv')
     instrumentList = pd.read_csv("https://api.kite.trade/instruments")
     fno_stocks = list(set(instrumentList[instrumentList['segment'] == 'NFO-FUT']["name"].to_list()))
     columns = full_table_df.columns
     nfo_stocks = pd.DataFrame(columns=columns) 


     for stock in fno_stocks:
        key_exists = stock in full_table_df['tradingsymbol'].values

        if key_exists:
             row = full_table_df[full_table_df['tradingsymbol'] == stock]
             nfo_stocks  = pd.concat([nfo_stocks, row], ignore_index=True)
        
     nfo_stocks.to_csv('csv/master/masterNFOTable.csv', index=False)
     json_final_df = nfo_stocks.to_dict(orient='records')
     filename_json = 'jsons/master/masterNFOTable.json'
     with open(filename_json, 'w') as json_file:
            json.dump(json_final_df, json_file, indent=4)
     

if __name__ == "__main__":
    kite = set_keys()
    get_instruments_kiteconnect()
    store_files()

    filePath  = 'csv/master/mapped_stock_sector.csv'
    if os.path.isfile(filePath) == False:
        map_stock_to_sector()

    seggregate_stocks_by_sector_and_Industry()
    get_NFO_stocks()

    #load_print_stocks_by_sector_and_Industry()




    
    
    
    

    
    




