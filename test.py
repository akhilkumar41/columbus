from kiteconnect import KiteConnect
import pickle
import pandas as pd
import time

# Initialize Kite Connect
apiKey = 'lstxlmmwrt85d63a'
apiSecret = 'kkspf857dt510x856cvjemzx7yaw6e2b'
tokenFile = 'access_token.pkl'

with open(tokenFile, 'rb') as f:
    access_token = pickle.load(f)

accessToken = access_token
kite = KiteConnect(api_key=apiKey)
kite.set_access_token(accessToken)

def place_limit_order(tradingsymbol, quantity, price):
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange='NFO',
            tradingsymbol=tradingsymbol,
            transaction_type='SELL',
            quantity=quantity,
            order_type='LIMIT',
            price=price,
            product=kite.PRODUCT_NRML,
            validity=kite.VALIDITY_DAY
        )
        print("Limit Order placed successfully. Order ID:", order_id)
        return order_id
    except Exception as e:
        print("Failed to place limit order:", str(e))
        return None

# Function to place stop-loss order
def place_stoploss_order(tradingsymbol, quantity, trigger_price, price):
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange='NFO',
            tradingsymbol=tradingsymbol,
            transaction_type='BUY',
            quantity=quantity,
            order_type='SL',
            price=price,
            trigger_price=trigger_price,
            product=kite.PRODUCT_NRML,
            validity=kite.VALIDITY_DAY
        )
        print("Stop-Loss Order placed successfully. Order ID:", order_id)
        return order_id
    except Exception as e:
        print("Failed to place stop-loss order:", str(e))
        return None

# Function to place target order
def place_target_order(tradingsymbol, quantity, price):
    try:
        order_id = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange='NFO',
            tradingsymbol=tradingsymbol,
            transaction_type='BUY',
            quantity=quantity,
            order_type='LIMIT',
            price=price,
            product=kite.PRODUCT_NRML,
            validity=kite.VALIDITY_DAY
        )
        print("Target Order placed successfully. Order ID:", order_id)
        return order_id
    except Exception as e:
        print("Failed to place target order:", str(e))
        return None

# Example usage

token = '10536962'
ltp = kite.ltp(token)
current_ltp = ltp[token]['last_price']


while current_ltp >= 413:
    print(current_ltp)
    ltp = kite.ltp(token)
    current_ltp = ltp[token]['last_price']
    time.sleep(0.2)



tradingsymbol = 'BANKNIFTY2461950000CE'  # Example trading symbol for Bank Nifty option
quantity = 15
limit_price = 413  # Your sell price
stoploss_trigger_price = 440  # Stop-loss trigger price
stoploss_price = 440  # Stop-loss price
target_price = 390  # Target price
exchange = 'NSE'


place_limit_order(tradingsymbol, quantity, limit_price)
place_stoploss_order(tradingsymbol, quantity, stoploss_trigger_price, stoploss_price)
place_target_order(tradingsymbol, quantity, target_price)



