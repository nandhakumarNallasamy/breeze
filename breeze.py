from breeze_connect import BreezeConnect
from datetime import datetime, timedelta, timezone
import logging
import pytz
from IPython.display import clear_output
import os
import threading

def get_ist_time():
    # Get IST
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now.replace(tzinfo=None)

def get_iso_date(dateString):
    # Get ISO date
    return get_ist_time().strftime("%Y-%m-%d") + 'T06:00:00.000Z'

def get_iso_datetime(dateString):
    # Get ISO dateTime
    return get_ist_time().strptime("%Y-%m-%dT%H:%M:%S.000Z")

def log():
    # Set up logging
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    return logger

def connect():
    api_key = os.environ.get("BREEZE_API_KEY")
    api_secret = os.environ.get("BREEZE_API_SECRET_KEY")
    breeze = BreezeConnect(api_key)
    try:
        breeze.generate_session(api_secret, session_token)
        logger.info("Successfully connected to BreezeConnect")
        
        def on_ticks(ticks):
            global current_pric
            try:
                if ticks.get("last"):
                    current_price = float(ticks["last"])
            except Exception as e:
                logger.error(f"Error in websocket callback: {e}")
        
        breeze.on_ticks = on_ticks
        breeze.ws_connect()
        logger.info("Websocket connected")
        return breeze
        
    except Exception as e:
        logger.error(f"Failed to generate session: {e}")
        exit(1)

def subscribe_feed(contract):
    try:
        expiry_date = datetime.strptime(contract.expiry_date.split('T')[0], '%Y-%m-%d').strftime('%d-%b-%Y')
        
        breeze.subscribe_feeds(
            exchange_code=contract.exchange_code,
            stock_code=contract.stock_code,
            product_type="Options" if contract.product_type == "options" else "Futures",
            expiry_date=expiry_date,
            strike_price=str(contract.strike_price),
            right="Call" if contract.right == "call" else "Put" if contract.right == "put" else "",
            get_exchange_quotes=True,
            get_market_depth=False
        )
        logger.info(f"Subscribed to feeds for {contract.shorthand}")
        
    except Exception as e:
        logger.error(f"Failed to subscribe feeds: {e}")
        raise

def unsubscribe_feed(contract):
    try:
        expiry_date = datetime.strptime(contract.expiry_date.split('T')[0], '%Y-%m-%d').strftime('%d-%b-%Y')
        
        breeze.unsubscribe_feeds(
            exchange_code=contract.exchange_code,
            stock_code=contract.stock_code,
            product_type="Options" if contract.product_type == "options" else "Futures",
            expiry_date=expiry_date,
            strike_price=str(contract.strike_price),
            right="Call" if contract.right == "call" else "Put" if contract.right == "put" else "",
            get_exchange_quotes=True,
            get_market_depth=False
        )
        logger.info(f"Unsubscribed from feeds for {contract.shorthand}")
        
    except Exception as e:
        logger.error(f"Failed to unsubscribe feeds: {e}")
        raise

def create_date(date, month):
    # Validate inputs
    if not (1 <= date <= 31) or not (1 <= month <= 12):
        raise ValueError("Invalid date or month")

    # Create a datetime object for the given date and month in the year 2024
    # We use 6:00 AM as the time
    dt = datetime(2024, month, date, 6, 0, 0)

    # Format the datetime as a string
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
def get_current_expiry():
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    days_ahead = (2 - today.weekday()) % 7  # 2 represents Wednesday
    wednesday = today + timedelta(days = days_ahead)
    return wednesday.strftime("%Y-%m-%dT06:00:00.000Z")

class contract:
    def __init__(self, stock_code, exchange_code, product_type, expiry_date, right="others", strike_price="0"):
        if product_type == "options":
            self.shorthand = f"{stock_code}-{expiry_date.split('T')[0]}-{strike_price}-{right}"
        else:
            self.shorthand = f"{stock_code}-FUT-{expiry_date.split('T')[0]}"
            
        self.stock_code = stock_code
        self.exchange_code = exchange_code
        self.product_type = product_type
        self.expiry_date = expiry_date
        self.right = right
        self.strike_price = strike_price
        
def generate_contracts(stock_code, expiry_date, start_strike, end_strike, interval, keyword=None, exchange_code="NFO", product_type="options"):
    for strike in range(start_strike, end_strike+1, interval):
        # Generating calls
        globals()[f"{stock_code}{strike}CE{keyword if keyword else ''}"] = contract(stock_code, exchange_code, product_type, expiry_date, "call", strike)
    
        # Generating puts
        globals()[f"{stock_code}{strike}PE{keyword if keyword else ''}"] = contract(stock_code, exchange_code, product_type, expiry_date, "put", strike)
        
        print(f"{f"{stock_code}{strike}CE{keyword if keyword else ''}"}, {f"{stock_code}{strike}PE{keyword if keyword else ''}"} generated.")
        
def clear():
    clear_output(wait=True)    

def place_fno_order(contract, action, quantity, count=1, price="0"):
    def place_single_order(contract, action, quantity, price):
        try:
            response = breeze.place_order(
                stock_code=contract.stock_code,
                exchange_code=contract.exchange_code,
                product=contract.product_type,
                action=action,
                order_type=("market" if price == "0" else "limit"),
                stoploss="",
                quantity=quantity,
                price=price,
                validity="day",
                validity_date=get_iso_date(get_ist_time()),
                disclosed_quantity="0",
                expiry_date=contract.expiry_date,
                right=contract.right,
                strike_price=contract.strike_price
            )
            if response.get("Status") == 200:
                logger.info(f"{contract.shorthand}-{action} order successful.")
            else:
                logger.error(f"{contract.shorthand}-{action} order failed.")
                if response.get("Error") is not None:
                    logger.error(f"Error details: {response.get('Error')}")
            return response
        except Exception as e:
            logger.error(f"Error in placing order:{contract.shorthand}-{action} {e}")
            return None

    if count <= 1:
        # If count is 1, just place the order directly without threading
        return place_single_order(contract, action, quantity, price)
    else:
        # Use threading for count orders
        threads = []
        responses = []

        for _ in range(count):
            thread = threading.Thread(target=lambda: responses.append(place_single_order(contract, action, quantity, price)))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

def get_price(contract):
    try:
        response = breeze.get_quotes(
            stock_code = contract.stock_code,
            exchange_code = contract.exchange_code,
            expiry_date = contract.expiry_date,
            product_type = contract.product_type,
            right = contract.right,
            strike_price =contract.strike_price
        )
        if response.get("Success"):
            return float(response["Success"][0]["ltp"])
        else:
            logger.error(f"API error in fetching {contract.shorthand}")
            print(response)
            return None
    except Exception as e:
        logger.error(f"Error fetching {contract.shorthand} price: {e}")
        return None

def place_hedge_order(contract1, contract2, quantity, count = 1):
    try:
        i = 0
        while i < count:
            response = place_fno_order(contract1, "buy", (quantity), price="0")
            if response.get("Status") == 200 :
                response = place_fno_order(contract2, "sell", (quantity), price="0")
                while response.get("Status") != 200 :
                    logger.info(f"Retrying...")
                    response = place_fno_order(contract2, "sell", (quantity), price="0")
            else:
                logger.info(f"Retrying...")
                i -= 1
            i += 1

    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error in loop: {e}")

def test_websocket(contract):
    global current_price
    try:
        subscribe_feed(contract)
        print(f"\nStarted monitoring {contract.shorthand}")
        print("Press Ctrl+C to stop...\n")
        
        last_price = None
        last_update_time = None
        
        while True:
            clear()
            now = datetime.now()
            
            if current_price != last_price:
                last_price = current_price
                last_update_time = now
                
            time_since_update = (now - last_update_time).total_seconds() if last_update_time else 0
                
            print(f"Contract: {contract.shorthand}")
            print(f"Current Price: {current_price}")
            print(f"Seconds since last update: {time_since_update:.1f}")
            sleep(0.1)
            
    except KeyboardInterrupt:
        print("\nTest stopped by user")
    finally:
        unsubscribe_feed(contract)
        current_price = None

# Main execution
if __name__ == "__main__":
    # Prompt session token
    session_token = input("Enter session token: ")
    
    # Initiate logger
    logger = log()
    logger.info(f"Current IST time: {get_ist_time()}")
    
    # Initialize BreezeConnect
    breeze = connect()
    
    # Get current ISO date
    current_date = get_iso_date(get_ist_time())
    CURRENT_EXPIRY = get_current_expiry()

    
