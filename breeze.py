from breeze_connect import BreezeConnect
from datetime import datetime, timedelta, timezone
import logging
import pytz
from IPython.display import clear_output
import os
import pandas as pd
import requests
from io import StringIO
import threading
from time import sleep
from concurrent.futures import ThreadPoolExecutor

# Global Variables
breeze = None
logger = None
contract_registry = {}
token_to_contract_map = {}
 
def get_ist_time():
    # Get IST
    utc_now = datetime.now(timezone.utc)
    ist_now = utc_now + timedelta(hours=5, minutes=30)
    return ist_now.replace(tzinfo=None)

def get_iso_date(dateString=None):
    # Get ISO date
    if dateString is None:
        return get_ist_time().strftime("%Y-%m-%d") + 'T06:00:00.000Z'
    
    try:
        date_obj = datetime.strptime(dateString, "%d-%b-%Y")
        return date_obj.strftime("%Y-%m-%d") + 'T06:00:00.000Z'
    except ValueError:
        logger.warning(f"❗ Invalid date format: {dateString}, using current date")
        return get_ist_time().strftime("%Y-%m-%d") + 'T06:00:00.000Z'

def get_iso_datetime(dateString=None):
    # Get ISO dateTime
    if dateString is None:
        return get_ist_time().strftime("%Y-%m-%dT%H:%M:%S.000Z")
    
    try:
        date_obj = datetime.strptime(dateString, "%d-%b-%Y %H:%M:%S")
        return date_obj.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    except ValueError:
        logger.warning(f"❗ Invalid datetime format: {dateString}, using current time")
        return get_ist_time().strftime("%Y-%m-%dT%H:%M:%S.000Z")

def convert_iso_to_breeze_date(iso_date):
    # Convert ISO format date to Breeze API format (DD-MMM-YYYY)
    # Input: "2025-03-27T06:00:00.000Z" or "2025-03-27"
    # Output: "27-Mar-2025"
    try:
        if 'T' in iso_date:
            iso_date = iso_date.split('T')[0]
        date_obj = datetime.strptime(iso_date, "%Y-%m-%d")
        return date_obj.strftime("%d-%b-%Y")
    except ValueError:
        if logger:
            logger.warning(f"❗ Invalid ISO date format: {iso_date}")
        return None

def log():
    # Set up logging
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    return logger

def connect():
    # Initialize and connect to BreezeConnect API with WebSocket
    global breeze, logger
    api_key = os.environ.get("BREEZE_API_KEY")
    api_secret = os.environ.get("BREEZE_API_SECRET_KEY")
    breeze = BreezeConnect(api_key)
    try:
        breeze.generate_session(api_secret, session_token)
        logger.info("✅ Successfully connected to BreezeConnect")
        
        def on_ticks(ticks):
            # Process incoming websocket data for both tick and OHLCV data
            try:
                # CASE 1: OHLCV data (identified by 'interval' field)
                if 'interval' in ticks:
                    # Extract common data
                    stock_code = ticks.get('stock_code')
                    
                    # Create OHLCV candle data structure
                    candle_data = {
                        'datetime': ticks.get('datetime'),
                        'open': float(ticks.get('open')),
                        'high': float(ticks.get('high')),
                        'low': float(ticks.get('low')),
                        'close': float(ticks.get('close')),
                        'volume': int(ticks.get('volume', 0))
                    }
                    
                    # Add open interest if available
                    if 'oi' in ticks:
                        candle_data['open_interest'] = int(ticks.get('oi', 0))
                    
                    expiry_date = None    
                    if 'expiry_date' in ticks:
                        expiry_date = ticks.get('expiry_date')  # Format: "DD-MMM-YYYY"
                    
                    # Handle options data (has right_type field)
                    if 'right_type' in ticks and ticks.get('strike_price'):
                        # Get options-specific data
                        strike_price = int(float(ticks.get('strike_price')))
                        right_type = ticks.get('right_type')  # CE or PE
                        
                        # Convert right_type to expected format
                        right = 'call' if right_type == 'CE' else 'put'
                        
                        # Find matching options contract using flexible approach
                        contract = None
                        
                        # Loop through registry to find the right contract
                        for reg_key, reg_contract in contract_registry.items():
                            # Check if this is the correct contract by matching:
                            # 1. The stock code (e.g., "NIFTY")
                            # 2. The strike price (e.g., "22000")
                            # 3. The right type (call/put)
                            if (stock_code in reg_key and str(strike_price).split('.')[0] in reg_key 
                                and right in reg_key.lower() and expiry_date in reg_key):
                                contract = reg_contract
                                break
                        
                        # If found, update the contract
                        if contract is not None:
                            # Add the candle to the contract's data
                            contract.ohlcv_data.append(candle_data)
                        else:
                            logger.error(f"❌ Error finding matching contract for {stock_code}-{expiry_date}-{strike_price}-{right}(OHLCV)")
                    
                    # Handle futures data
                    else:
                        # Find matching futures contract
                        contract = None
                        
                        # Loop through registry to find futures contract
                        for reg_key, reg_contract in contract_registry.items():
                            if stock_code in reg_key and "FUT" in reg_key and expiry_date in reg_key:
                                contract = reg_contract
                                break
                        
                        # If found, update the contract
                        if contract:
                            # Add the candle to the contract's data
                            contract.ohlcv_data.append(candle_data)
                        else:
                            logger.error(f"❌ Error finding matching contract for {stock_code}-FUT-{expiry_date}(OHLCV)")
                
                # CASE 2: Tick data (has 'last' field but no 'interval')
                elif 'last' in ticks:
                    # Add this part to handle symbol-based tick data
                    if 'symbol' in ticks:
                        # Extract the token ID from the symbol (e.g., from '4.1!35559' get '35559')
                        token_id = ticks['symbol'].split('!')[1] if '!' in ticks['symbol'] else None
                        
                        if token_id and token_id in token_to_contract_map:
                            # Get contract info from the mapping
                            contract_info = token_to_contract_map[token_id]
                            
                            # Create registry key based on the contract details already parsed
                            registry_key = None
                            
                            if contract_info.get('product_type') == 'options':
                                # Format: "{stock_code}-{expiry_date}-{strike_price}-{right}"
                                registry_key = f"{contract_info['stock_code']}-{contract_info['expiry_date']}-{contract_info['strike_price']}-{contract_info['right']}"
                            elif contract_info.get('product_type') == 'futures':
                                # Format: "{stock_code}-FUT-{expiry_date}"
                                registry_key = f"{contract_info['stock_code']}-FUT-{contract_info['expiry_date']}"
                            
                            # Look up contract in registry
                            contract = None
                            if registry_key in contract_registry:
                                contract = contract_registry[registry_key]
                            else:
                                # Try a more flexible lookup
                                for reg_key, reg_contract in contract_registry.items():
                                    if contract_info['stock_code'] in reg_key:
                                        if contract_info.get('product_type') == 'options':
                                            if (contract_info['strike_price'] in reg_key and 
                                                contract_info['right'] in reg_key.lower() and 
                                                contract_info['expiry_date'] in reg_key):
                                                contract = reg_contract
                                                break
                                        elif contract_info.get('product_type') == 'futures':
                                            if "FUT" in reg_key and contract_info['expiry_date'] in reg_key:
                                                contract = reg_contract
                                                break
                            
                            # Update contract data if found
                            if contract:
                                contract.ltp = float(ticks['last'])
                                contract.last_update_time = datetime.now()
                            else:
                                logger.debug(f"❌ Error finding matching contract for {stock_code}-{expiry_date}-{strike_price}-{right}(Tick-by_Tick)")
                
            except Exception as e:
                logger.error(f"❌ Error in websocket callback: {e}")
                import traceback
                traceback.print_exc()
        
        breeze.on_ticks = on_ticks
        breeze.ws_connect()
        logger.info("✅ Websocket connected")
        return breeze
        
    except Exception as e:
        logger.error(f"❌ Failed to generate session: {e}")
        exit(1)
        
# Function to download and prepare the token to contract details mapping
def load_token_mapping(url="https://traderweb.icicidirect.com/Content/File/txtFile/ScripFile/StockScriptNew.csv"):
    try:
        logger.info(f"🌐 Downloading stock script CSV from {url}")
        response = requests.get(url)
        df = pd.read_csv(StringIO(response.text))
        
        for _, row in df.iterrows():
            # Only process rows with valid TK values
            if pd.notna(row['TK']) and row['TK'] != 0:
                # Convert TK to string to ensure consistent lookup
                tk = str(row['TK'])
                
                
                # Get stock code from SM field
                sm = str(row['SM'])
                
                # Parse SM field to extract all contract details
                if '~' in sm:
                    parts = sm.split('~')
                    stock_code = parts[0]
                    
                    if len(parts) > 1 and ':' in parts[1]:
                        # Parse contract specification (e.g., "O:18-Mar-2025:CE:7400000")
                        contract_specs = parts[1].split(':')
                        
                        if len(contract_specs) >= 2:
                            contract_type = contract_specs[0]  # 'O' for options, 'F' for futures
                            expiry_date = contract_specs[1]    # Format: DD-MMM-YYYY
                            
                            if contract_type == 'O' and len(contract_specs) >= 4:
                                # This is an options contract
                                right_code = contract_specs[2]  # 'CE' or 'PE'
                                right = 'call' if right_code == 'CE' else 'put'
                                3
                                # Strike price needs to be divided by 100
                                strike_raw = contract_specs[3]
                                try:
                                    strike_price = str(int(int(strike_raw) / 100))
                                except (ValueError, TypeError):
                                    strike_price = strike_raw
                                
                                token_to_contract_map[tk] = {
                                    'stock_code': stock_code,
                                    'expiry_date': expiry_date,
                                    'strike_price': strike_price,
                                    'right': right,
                                    'product_type': 'options'
                                }
                                
                            elif contract_type == 'F':
                                # This is a futures 2.6ontract
                                token_to_contract_map[tk] = {
                                    'stock_code': stock_code,
                                    'expiry_date': expiry_date,
                                    'product_type': 'futures'
                                }
                else:
                    # If SM doesn't have extended info, just store the stock code
                    token_to_contract_map[tk] = {'stock_code': sm}
        
        logger.info(f"✅ Loaded {len(token_to_contract_map)} token mappings")
        return True
    except Exception as e:
        logger.error(f"❌ Error loading token mappings: {e}")
        return False


def subscribe_feed(contract, interval=None):
    #Subscribe to real-time feed for a contract
    
    if interval and contract.is_ohlcv_subscribed:
        logger.info(f"ℹ️ {contract.short_hand} is already subscribed")
        return
    elif not interval and contract.is_ltp_subscribed:
        logger.info(f"ℹ️ {contract.short_hand} is already subscribed")
        return

    try:
        # Convert ISO date to Breeze format using utility function
        expiry_date = convert_iso_to_breeze_date(contract.expiry_date)
        
        # Build parameters dictionary
        params = {
            "exchange_code" : contract.exchange_code,
            "stock_code" : contract.stock_code,
            "product_type" : "Options" if contract.product_type == "options" else "Futures",
            "expiry_date" : expiry_date,
            "strike_price" : str(contract.strike_price),
            "right" : "Call" if contract.right == "call" else "Put" if contract.right == "put" else "",
            "get_exchange_quotes" : True,
            "get_market_depth" : False
        }

        # Conditionally add interval
        if interval is not None:
            params["interval"] = interval

        # Call API with unpacked parameters
        breeze.subscribe_feeds(**params)
        
        # Register this contract so the callback can find it
        contract_registry[contract.short_hand] = contract
            
        if interval:
            contract.is_ohlcv_subscribed = True
        else:
            contract.is_ltp_subscribed = True

        logger.info(f"✅ Subscribed to feeds for {contract.short_hand}")
        
    except Exception as e:
        logger.error(f"❌ Failed to subscribe feeds for {contract.short_hand}: {e}")
        raise

def unsubscribe_feed(contract, interval=None):
    #Unsubscribe from real-time feed for a contract
    
    if interval and not contract.is_ohlcv_subscribed:
        logger.info(f"ℹ️ {contract.short_hand} is not subscribed")
        return
    elif not interval and not contract.is_ltp_subscribed:
        logger.info(f"ℹ️ {contract.short_hand} is not subscribed")
        return

    try:
        # Convert ISO date to Breeze format using utility function
        expiry_date = convert_iso_to_breeze_date(contract.expiry_date)
        
        # Build parameters dictionary
        params = {
            "exchange_code" : contract.exchange_code,
            "stock_code" : contract.stock_code,
            "product_type" : "Options" if contract.product_type == "options" else "Futures",
            "expiry_date" : expiry_date,
            "strike_price" : str(contract.strike_price),
            "right" : "Call" if contract.right == "call" else "Put" if contract.right == "put" else "",
            "get_exchange_quotes" : True,
            "get_market_depth" : False
        }

        # Conditionally add interval
        if interval is not None:
            params["interval"] = interval

        # Call API with unpacked parameters
        breeze.unsubscribe_feeds(**params)
        
        if interval:
            contract.is_ohlcv_subscribed = False 
            contract.ohlcv_data = []
        else: 
            contract.is_ltp_subscribed = False
            # Reset the LTP and last update time to None when unsubscribing from price feed
            contract.ltp = None
            contract.last_update_time = None

        # Unregister this contract
        if contract.short_hand in contract_registry and not contract.is_ltp_subscribed and not contract.is_ohlcv_subscribed:
            del contract_registry[contract.short_hand]

        logger.info(f"✅ Unsubscribed from feeds for {contract.short_hand}")
        
    except Exception as e:
        logger.error(f"❌ Failed to unsubscribe feeds for {contract.short_hand}: {e}")
        raise

def subscribe_multiple_feeds(contracts, interval=None):
    #Subscribe to multiple feeds at once
    for contract in contracts:
        subscribe_feed(contract, interval)
        
    return

def unsubscribe_multiple_feeds(contracts, interval=None):
    #Unsubscribe from multiple feeds at once
    for contract in contracts:
        unsubscribe_feed(contract, interval)
        
    return
        
def unsubscribe_all_feed(interval=None):
    # Unsubscribe from all web socket feeds
    unsubscribe_multiple_feeds(list(contract_registry.values()), interval)
    
def create_date(date, month, year=2025):
    # Create ISO8601 date string for the given date and month
    if not (1 <= date <= 31) or not (1 <= month <= 12):
        raise ValueError("Invalid date or month")
    dt = datetime(year, month, date, 6, 0, 0)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
def get_current_expiry(target_weekday):
    # Get the date of the current or next occurrence of the specified weekday
    # 0-Monday, 1-Tuesday, 2-Wednesday,..., 6-Sunday
    today = datetime.now(pytz.timezone('Asia/Kolkata'))
    days_ahead = (target_weekday - today.weekday()) % 7  
    weekday = today + timedelta(days = days_ahead)
    print(weekday)
    return weekday.strftime("%Y-%m-%dT06:00:00.000Z")

class contract:
    # Class to represent a trading contract (futures or options)
    def __init__(self, stock_code, exchange_code, product_type, expiry_date, right="others", strike_price="0"):
        self.stock_code = stock_code
        self.exchange_code = exchange_code
        self.product_type = product_type
        self.expiry_date = expiry_date
        self.right = right
        self.strike_price = strike_price
        
        # Create short_hand with the Breeze format for expiry date
        if product_type == "options":
            self.short_hand = f"{stock_code}-{convert_iso_to_breeze_date(expiry_date)}-{strike_price}-{right}"
        else:
            self.short_hand = f"{stock_code}-FUT-{convert_iso_to_breeze_date(expiry_date)}"
        
        # Essential fields
        self.ltp = None
        self.last_update_time = None
        self.is_ltp_subscribed = False
        self.is_ohlcv_subscribed = False
        self.ohlcv_interval = None
        
        # OHLCV candle storage
        self.ohlcv_data = []
        self.max_candles = 1000  # Maximum candles to store
        
        # Limit the size of the candle list
        if len(self.ohlcv_data) > self.max_candles:
            self.ohlcv_data = self.ohlcv_data[-self.max_candles:]
            
        # Store signals generated by ML function
        self.signal = None
        self.confidence = None
        
def generate_contracts(stock_code, expiry_date, start_strike, end_strike, interval, keyword=None, exchange_code="NFO", product_type="options"):
    contracts = []
    
    # Generate call and put option contracts for a range of strike prices
    for strike in range(start_strike, end_strike+1, interval):
        # Generating calls
        call_contract = contract(stock_code, exchange_code, product_type, expiry_date, "call", strike)
        globals()[f"{stock_code}{strike}CE{keyword if keyword else ''}"] = call_contract
        contracts.append(call_contract)
        
        # Generating puts
        put_contract = contract(stock_code, exchange_code, product_type, expiry_date, "put", strike)
        globals()[f"{stock_code}{strike}PE{keyword if keyword else ''}"] = put_contract
        contracts.append(put_contract)
            
        print(f"ℹ️ {f"{stock_code}{strike}CE{keyword if keyword else ''}"}, {f"{stock_code}{strike}PE{keyword if keyword else ''}"} generated.")
        
    return contracts

def clear():
    # Clear the output in Jupyter notebooks
    clear_output(wait=True)    

def place_fno_order(contract, action, quantity, count=1, price="0", stoploss="0"):
    # Place an F&O order, optionally multiple times using threading
    def place_single_order(contract, action, quantity, price, stoploss):
        try:
            response = breeze.place_order(
                stock_code=contract.stock_code,
                exchange_code=contract.exchange_code,
                product=contract.product_type,
                action=action,
                order_type=("market" if price == "0" else "limit"),
                stoploss=stoploss,
                quantity=quantity,
                price=price,
                validity="day",
                validity_date=get_iso_date(),
                disclosed_quantity="0",
                expiry_date=contract.expiry_date,
                right=contract.right,
                strike_price=contract.strike_price
            )
            if response.get("Status") == 200:
                logger.info(f"✅ {contract.short_hand}-{action} order successful.")
            else:
                logger.error(f"❌ {contract.short_hand}-{action} order failed.")
                if response.get("Error") is not None:
                    logger.error(f"❌ Error details: {response.get('Error')}")
            return response
        except Exception as e:
            logger.error(f"❌ Error in placing order:{contract.short_hand}-{action} {e}")
            return None

    if count <= 1:
        # If count is 1, just place the order directly without threading
        return place_single_order(contract, action, quantity, price, stoploss)
    else:
        # Use threading for count orders
        threads = []
        responses = []

        for _ in range(count):
            thread = threading.Thread(target=lambda: responses.append(place_single_order(contract, action, quantity, price, stoploss)))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
            
        return responses

def get_price(contract):
    # Get the current price for a contract
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
            logger.error(f"❌ API error in fetching {contract.short_hand}")
            print(response)
            return None
    except Exception as e:
        logger.error(f"❌ Error fetching {contract.short_hand} price: {e}")
        return None

def place_hedge_order(contract1, contract2, quantity, count = 1):
    # Place a hedge order (buy one contract, sell another) multiple times
    try:
        i = 0
        while i < count:
            response = place_fno_order(contract1, "buy", (quantity), price="0")
            if response.get("Status") == 200 :
                response = place_fno_order(contract2, "sell", (quantity), price="0")
                while response.get("Status") != 200 :
                    logger.info(f"🔄 Retrying...")
                    response = place_fno_order(contract2, "sell", (quantity), price="0")
            else:
                logger.info(f"🔄 Retrying...")
                i -= 1
            i += 1

    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
    except Exception as e:
        logger.error(f"❌ Unexpected error in loop: {e}")

def hold_spot(sell_contract, sell_quantity, sell_multiple, threshold=0, buy_contract=None, buy_quantity=None, buy_multiple=None, position=None):
    # Monitor and trade a contract based on price movements around a threshold
    try:
        position = position if position is not None else 0
        count = 0
        running = True
        iterations = 0

        # Subscribe to feeds to get price updates
        subscribe_feed(sell_contract)
        
        # Wait for price data to be available
        while sell_contract.ltp is None:
            sleep(0.1)
            clear()
            logger.warning(f"⚠️ No price data received for {sell_contract.short_hand}")
            
           
        # Get initial price if threshold not specified
        if threshold == 0:
            threshold = sell_contract.ltp
            logger.info(f"✅ Using threshold: {threshold}")

        # Place initial buy order if specified
        if buy_contract and buy_quantity and buy_multiple:
            place_fno_order(buy_contract, "buy", buy_quantity, buy_multiple)

        # Main trading loop
        while running:
            iterations += 1
           
            if iterations % 10 == 0:
                clear_output(wait=True)
                print(f"Threshold: {threshold} | Price: {sell_contract.ltp} | Count: {count} | Position: {position} | Iterations: {iterations}")
           
            if sell_contract.ltp is not None:
                if position == 0 and sell_contract.ltp < threshold-1:
                    place_fno_order(sell_contract, "sell", sell_quantity, sell_multiple)
                    position = -1
                    count += 1
                    logger.info(f"✅ Sold at {sell_contract.ltp}")
                   
                elif position == -1 and sell_contract.ltp > threshold:
                    place_fno_order(sell_contract, "buy", sell_quantity, sell_multiple)
                    position = 0
                    logger.info(f"✅ Bought at {sell_contract.ltp}")
               
            sleep(0.2)
               
    except KeyboardInterrupt:
        logger.info("Operation terminated by user.")
    except Exception as e:
        logger.error(f"❌ Error in main loop: {e}")
    finally:
        running = False
        unsubscribe_feed(sell_contract)

def hold_buy(contract, qunatity, multiple, threshold=0, stop_loss=5, position=0):
    # Monitor and trade a contract based on price movements around a threshold
    try:
        count = 0
        running = True
        iterations = 0
        
        # Subscribe to feeds to get price updates
        subscribe_feed(contract)
        
        # Wait for price data to be available
        while contract.ltp is None:
            sleep(0.1)
            clear()
            logger.warning(f"⚠️ No price data received for {contract.short_hand}")
           
        # Get initial price if threshold not specified
        if threshold == 0:
            threshold = contract.ltp
            logger.info(f"✅ Using threshold: {threshold}")
            
        # Main trading loop
        while running:
            iterations += 1
           
            if iterations % 10 == 0:
                clear_output(wait=True)
                print(f"Threshold: {threshold} | Price: {contract.ltp} | Count: {count} | Iterations: {iterations}")
           
            if contract.ltp is not None:
                if position == 0 and contract.ltp > threshold:
                    place_fno_order(contract, "buy", qunatity, multiple)
                    position = 1
                    logger.info(f"✅ Bought at {contract.ltp}")
                elif position == 1 and contract.ltp < threshold-stop_loss:
                    place_fno_order(contract, "sell", qunatity, multiple)
                    position = 0
                    count += 1
                    logger.info(f"✅ Sold at {contract.ltp}")
                if(contract.ltp > threshold+50):
                    threshold += 25
               
            sleep(0.2)
               
    except KeyboardInterrupt:
        logger.info("Operation terminated by user.")
    except Exception as e:
        logger.error(f"❌ Error in main loop: {e}")
    finally:
        running = False
        if position == 1:
            place_fno_order(contract, "sell", qunatity, multiple)
        unsubscribe_feed(contract)

def test_websocket(contracts):
    #Test websocket connection by monitoring price updates for multiple contracts
    
    # Ensure contracts is a list even if a single contract was passed
    if not isinstance(contracts, list):
        contracts = [contracts]
    
    try:
        # Subscribe to all contracts
        for contract in contracts:
            subscribe_feed(contract)
        
        print(f"\n✅ Started monitoring {len(contracts)} contracts")
        print("Press Ctrl+C to stop...\n")
        
        # Dictionary to store last update info for each contract
        last_updates = {contract.short_hand: {"price": None, "time": None} for contract in contracts}
        
        while True:
            clear()
            now = datetime.now()
            
            # Print header
            print(f"{'Contract':<25} {'Price':<10} {'Last Update':<15}")
            print("-" * 55)
            
            # Update and show data for each contract
            for contract in contracts:
                last_data = last_updates[contract.short_hand]
                
                # Check if price has changed
                if contract.ltp != last_data["price"]:
                    last_data["price"] = contract.ltp
                    last_data["time"] = now
                
                # Calculate time since last update
                time_since_update = ""
                if last_data["time"]:
                    seconds = (now - last_data["time"]).total_seconds()
                    time_since_update = f"{seconds:.1f}s ago"
                
                # Print contract info
                print(f"{contract.short_hand:<25} {contract.ltp if contract.ltp else 'No data':<10} {time_since_update:<15}")
            
            sleep(0.2)
            
    except KeyboardInterrupt:
        print("\nTest stopped by user")
    finally:
        # Make sure to unsubscribe from all feeds
        for contract in contracts:
            unsubscribe_feed(contract)

def place_exit_order(position_tuple):
    # Place a single exit order, splitting into chunks if necessary
    pos, action = position_tuple
    try:
        price_adjustment = 0
        price = float(pos["ltp"])
        qty = int(float(pos["quantity"]))
        if price > 15:
            price_adjustment = 15 if action == "buy" else -15
        price += price_adjustment
        max_qty = 600  # Maximum order size
        
        # Convert expiry date using utility function
        expiry_date = get_iso_date(pos["expiry_date"])
        
        while qty > 0:
            chunk_qty = min(qty, max_qty)
            qty -= chunk_qty

            response = breeze.square_off(
                exchange_code=str(pos["exchange_code"]),
                product=str(pos["product_type"]).lower(),
                stock_code=str(pos["stock_code"]),
                expiry_date=str(expiry_date),
                right=str(pos["right"]),
                strike_price=str(pos["strike_price"]),
                action=str(action),
                order_type="limit",  # Changed to limit since we're using price
                validity="day",
                stoploss="0",
                quantity=str(chunk_qty),
                price=str(price),  # Using adjusted price
                validity_date=str(get_iso_date()),
                trade_password="",
                disclosed_quantity="0"
            )
            
            if response.get("Status") == 200:
                logger.info(f"✅ {action.upper()}: {pos['stock_code']} {pos['strike_price']}{pos['right']} x {chunk_qty} @ {price}")
            else:
                logger.error(f"❌ Failed {action.upper()}: {pos['stock_code']} {pos['strike_price']}{pos['right']} x {chunk_qty} @ {price}: {response.get('Error', 'Unknown error')}")

    except Exception as e:
        logger.error(f"❌ Order failed - {pos['stock_code']}: {str(e)}")

def cancel_order(order):
    # Cancel a single order with logging
    try:
        breeze.cancel_order(order["exchange_code"], order["order_id"])
        logger.info(f"✅ Cancelled: {order['stock_code']} {order['strike_price']}{order['right']} (ID: {order['order_id']})")
    except Exception as e:
        logger.error(f"❌ Cancel failed - Order {order['order_id']}: {str(e)}")

def az5():
    # Emergency close all positions and cancel pending orders
    try:
        logger.info("🔄 AZ5 initiated")
        
        close_open_positions()
        
        cancel_pending_orders()
        
        logger.info("✅ AZ5 completed")
        
    except Exception as e:
        logger.error(f"❌ AZ5 failed: {e}")
        
def close_open_positions():
    # Close positions
        positions_response = breeze.get_portfolio_positions()
        if positions_response.get("Error") is None:  # No error means we have valid response
            positions = positions_response.get("Success", [])
            if positions:  # If we have positions data
                active = [(p, "buy" if p["action"] == "Sell" else "sell") 
                         for p in positions 
                         if p["quantity"] != "0" and p["action"] != "NA"]
                
                if active:
                    logger.info(f"🔄 Processing {len(active)} positions")
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        list(executor.map(place_exit_order, active))
                else:
                    logger.info("ℹ️ No active positions to close")
        else:
            logger.info("ℹ️ No positions available")

def cancel_pending_orders():
    # Cancel pending orders for both NFO and BSE
    for exchange_code in ["NFO", "BFO"]:
        orders_response = breeze.get_order_list(
            exchange_code=exchange_code,
            from_date=get_iso_date(),
            to_date=get_iso_date()
        )
        
        if orders_response.get("Error") is None:  # No error means we have valid response
            orders = orders_response.get("Success", [])
            pending = [o for o in orders if o["status"] in ["Ordered", "Requested"]]
            if pending:
                logger.info(f"🔄 Cancelling {len(pending)} {exchange_code} pending orders")
                with ThreadPoolExecutor(max_workers=5) as executor:
                    list(executor.map(cancel_order, pending))
            else:
                logger.info(f"ℹ️ No pending {exchange_code} orders to cancel")
        else:
            logger.info(f"ℹ️ No {exchange_code} orders available")

def get_historical_data(contract, start_datetime, end_datetime, interval="1minute"):
    # Get historical data for a contract for a specific time range
    
    # Parameters:
    # start_datetime & end_datetime in format "DD-MMM-YYYY HH:MM:SS" (e.g. "13-Mar-2025 09:30:00")
    # end_datetime - End time in format "DD-MMM-YYYY HH:MM:SS" (e.g. "13-Mar-2025 15:30:00")
    # interval - Data interval: "1second", "1minute", "5minute", "30minute" or "1day"
    
    # Returns:
    # pandas DataFrame with OHLCV data or None if there was an error
    
    try:
        # Convert input datetime strings to ISO format using existing utility functions
        start_time = get_iso_datetime(start_datetime)
        end_time = get_iso_datetime(end_datetime)
        
        logger.info(f"🔄 Fetching historical data for {contract.short_hand}")
        logger.info(f"🕒 Time range: {start_datetime} to {end_datetime}")
        logger.info(f"📊 Using interval: {interval}")
        
        # Get historical data
        response = breeze.get_historical_data_v2(
            interval=interval,
            from_date=start_time,
            to_date=end_time,
            stock_code=contract.stock_code,
            exchange_code=contract.exchange_code,
            product_type=contract.product_type,
            expiry_date=contract.expiry_date,
            right=contract.right,
            strike_price=contract.strike_price
        )
        
        # Check if successful
        if "Success" in response and len(response["Success"]) > 0:
            # Convert to DataFrame
            data = pd.DataFrame(response["Success"])
            
            # Format datetime and numeric columns
            data['datetime'] = pd.to_datetime(data['datetime'])
            for col in ['open', 'high', 'low', 'close']:
                data[col] = pd.to_numeric(data[col])
            
            # Convert volume and open interest to numeric and integers
            data['volume'] = pd.to_numeric(data['volume']).astype('int')
            if 'oi' in data.columns:
                data['oi'] = pd.to_numeric(data['oi']).astype('int')
            
            logger.info(f"✅ Retrieved {len(data)} data points")
            
            return data
        else:
            logger.error("❌ No data returned or error in API response")
            if "Error" in response:
                logger.error(f"❌ API Error: {response['Error']}")
            return None
            
    except Exception as e:
        logger.error(f"❌ Error fetching historical data: {e}")
        return None
    
def plot_historical_data(data, contract, show_volume=True):
    # Plot historical data for a contract
    try:
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle
        import pandas as pd
        
        if data is None or len(data) == 0:
            logger.error("❌ No data to plot")
            return
            
        # Create figure with appropriate subplots
        if show_volume:
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), 
                                         gridspec_kw={'height_ratios': [3, 1]})
        else:
            fig, ax1 = plt.subplots(1, 1, figsize=(12, 6))
        
        # Determine the interval for appropriate visualization
        is_intraday = (data['datetime'].max() - data['datetime'].min()).total_seconds() <= 24*60*60
        
        # Plot price data
        if len(data) > 1 and is_intraday:
            # Candlestick chart for intraday data
            for i, row in data.iterrows():
                # Candlestick color
                color = 'green' if row['close'] >= row['open'] else 'red'
                
                # Draw body
                body_height = abs(row['close'] - row['open'])
                body_bottom = min(row['open'], row['close'])
                rect = Rectangle((i-0.4, body_bottom), 0.8, body_height, 
                                color=color, alpha=0.8, zorder=2)
                ax1.add_patch(rect)
                
                # Draw wicks
                ax1.plot([i, i], [row['low'], body_bottom], color='black', linewidth=1, zorder=1)
                ax1.plot([i, i], [body_bottom+body_height, row['high']], color='black', linewidth=1, zorder=1)
            
            # Set x-ticks to datetime
            ax1.set_xticks(range(0, len(data), max(1, len(data)//10)))
            ax1.set_xticklabels(data['datetime'].iloc[::max(1, len(data)//10)].dt.strftime('%H:%M'), rotation=45)
        else:
            # Line chart for daily data or single point
            ax1.plot(data.index, data['close'], label='Close Price', color='blue', linewidth=1.5)
            
        ax1.set_title(f"{contract.short_hand} - {data['datetime'].min().strftime('%Y-%m-%d')} to {data['datetime'].max().strftime('%Y-%m-%d')}")
        ax1.set_ylabel('Price')
        ax1.grid(True, alpha=0.3)
        
        # Add volume subplot if requested
        if show_volume:
            ax2.bar(range(len(data)), data['volume'], color='navy', alpha=0.5)
            ax2.set_ylabel('Volume')
            ax2.grid(True, alpha=0.3)
            ax2.set_xticks(range(0, len(data), max(1, len(data)//10)))
            ax2.set_xticklabels(data['datetime'].iloc[::max(1, len(data)//10)].dt.strftime('%H:%M'), rotation=45)
        
        plt.tight_layout()
        plt.show()
        
    except Exception as e:
        logger.error(f"❌ Error plotting data: {e}")
        
def run_signal_generator(contracts, stop_event, min_candles=60, update_interval=1, max_workers=10):
    # Continuously update trading signals for contracts in parallel threads with optimized parameters.
    
    logger.info(f"Starting signal generator for {len(contracts)} contracts")
    
    # Function to process a single contract with optimized parameters
    def process_contract(contract):
        try:
            # Skip if insufficient data
            if not hasattr(contract, 'ohlcv_data') or not contract.ohlcv_data:
                return
            
            # Get data as DataFrame
            if isinstance(contract.ohlcv_data, list):
                if len(contract.ohlcv_data) < min_candles:
                    return
                df = pd.DataFrame(contract.ohlcv_data)
            else:
                if len(contract.ohlcv_data) < min_candles:
                    return
                df = contract.ohlcv_data.copy()
            
            # Verify columns
            if 'datetime' not in df.columns:
                return
            
            required_columns = ['open', 'high', 'low', 'close']
            if not all(col in df.columns for col in required_columns):
                return
            
            # Prepare data
            df['datetime'] = pd.to_datetime(df['datetime'])
            for col in required_columns:
                df[col] = pd.to_numeric(df[col])
            
            if 'volume' in df.columns:
                df['volume'] = pd.to_numeric(df['volume'])
            
            df = df.sort_values('datetime').reset_index(drop=True)
            df = df.tail(min_candles)
            
            # Calculate indicators
            indicators = {}
            indicators['current_price'] = df['close'].iloc[-1]
            indicators['price_change'] = df['close'].iloc[-1] / df['close'].iloc[-2] - 1
            
            for window in [5, 10, 20]:
                if len(df) >= window:
                    indicators[f'ma_{window}'] = df['close'].rolling(window=window).mean().iloc[-1]
                    indicators[f'close_to_ma_{window}'] = df['close'].iloc[-1] / indicators[f'ma_{window}'] - 1
            
            if len(df) >= 26:
                ema_12 = df['close'].ewm(span=12, adjust=False).mean()
                ema_26 = df['close'].ewm(span=26, adjust=False).mean()
                indicators['macd'] = ema_12.iloc[-1] - ema_26.iloc[-1]
                
                if len(df) >= 35:
                    macd_series = ema_12 - ema_26
                    indicators['macd_signal'] = macd_series.ewm(span=9, adjust=False).mean().iloc[-1]
            
            if len(df) >= 15:
                delta = df['close'].diff()
                gain = delta.where(delta > 0, 0)
                loss = -delta.where(delta < 0, 0)
                avg_gain = gain.rolling(window=14).mean().iloc[-1]
                avg_loss = loss.rolling(window=14).mean().iloc[-1]
                
                if avg_loss != 0:
                    rs = avg_gain / avg_loss
                    indicators['rsi'] = 100 - (100 / (1 + rs))
                else:
                    indicators['rsi'] = 100
            
            if len(df) >= 20:
                indicators['bb_middle'] = df['close'].rolling(window=20).mean().iloc[-1]
                indicators['bb_std'] = df['close'].rolling(window=20).std().iloc[-1]
                indicators['bb_upper'] = indicators['bb_middle'] + 2 * indicators['bb_std']
                indicators['bb_lower'] = indicators['bb_middle'] - 2 * indicators['bb_std']
                indicators['bb_position'] = (df['close'].iloc[-1] - indicators['bb_lower']) / (indicators['bb_upper'] - indicators['bb_lower'])
            
            # Calculate VWAP
            if 'volume' in df.columns:
                df['vwap'] = (df['volume'] * ((df['high'] + df['low'] + df['close']) / 3)).cumsum() / df['volume'].cumsum()
                indicators['vwap'] = df['vwap'].iloc[-1]
                indicators['price_to_vwap'] = df['close'].iloc[-1] / indicators['vwap'] - 1

            # Generate signal with weighted indicators
            # Weights based on observed performance: Increase BB and RSI, decrease MA
            weights = {
                'ma': 0.5,       # Reduced weight for Moving Averages
                'rsi': 1.5,      # Increased weight for RSI
                'macd': 1.0,     # Normal weight for MACD
                'bb': 1.5,       # Increased weight for Bollinger Bands
                'price': 1.0,    # Normal weight for Price Change
                'vwap': 1.0      # Normal weight for VWAP
            }
            
            buy_votes = sell_votes = 0
            total_weight = 0
            
            # Moving Average vote (reduced weight)
            if 'ma_5' in indicators and 'ma_10' in indicators:
                weight = weights['ma']
                total_weight += weight
                if indicators['close_to_ma_5'] > 0 and indicators['ma_5'] > indicators['ma_10']:
                    buy_votes += weight
                elif indicators['close_to_ma_5'] < 0 and indicators['ma_5'] < indicators['ma_10']:
                    sell_votes += weight
            
            # RSI vote (increased weight)
            if 'rsi' in indicators:
                weight = weights['rsi']
                total_weight += weight
                if indicators['rsi'] < 30:
                    buy_votes += weight
                elif indicators['rsi'] > 70:
                    sell_votes += weight
            
            # MACD vote
            if 'macd' in indicators and 'macd_signal' in indicators:
                weight = weights['macd']
                total_weight += weight
                if indicators['macd'] > indicators['macd_signal']:
                    buy_votes += weight
                elif indicators['macd'] < indicators['macd_signal']:
                    sell_votes += weight
            
            # Bollinger Bands vote (increased weight)
            if 'bb_position' in indicators:
                weight = weights['bb']
                total_weight += weight
                if indicators['bb_position'] < 0.2:
                    buy_votes += weight
                elif indicators['bb_position'] > 0.8:
                    sell_votes += weight
            
            # Price change vote
            if 'price_change' in indicators:
                weight = weights['price']
                total_weight += weight
                if indicators['price_change'] > 0.003:  # Reduced from 0.005 to 0.003
                    buy_votes += weight
                elif indicators['price_change'] < -0.003:  # Reduced from -0.005 to -0.003
                    sell_votes += weight
                    
            # VWAP vote
            if 'vwap' in indicators:
                weight = weights['vwap']
                total_weight += weight
                if df['close'].iloc[-1] > indicators['vwap']:
                    buy_votes += weight
                elif df['close'].iloc[-1] < indicators['vwap']:
                    sell_votes += weight
            
            # Calculate final signal
            buy_percentage = buy_votes / total_weight if total_weight > 0 else 0
            sell_percentage = sell_votes / total_weight if total_weight > 0 else 0
            
            # Lowered threshold criteria (from >0.5 to >0.45)
            if buy_percentage > 0.45 and buy_percentage > sell_percentage:
                signal = "BUY"
                confidence = buy_percentage
            elif sell_percentage > 0.45 and sell_percentage > buy_percentage:
                signal = "SELL"
                confidence = sell_percentage
            else:
                signal = "HOLD"
                confidence = 1 - (buy_percentage + sell_percentage)
            
            # Update contract
            old_signal = getattr(contract, 'signal', None)
            contract.signal = signal
            contract.confidence = confidence
            
            # Log significant changes
            if old_signal != signal and confidence > 0.5:
                contract_id = getattr(contract, 'short_hand', f"{contract.stock_code}-{contract.strike_price}-{contract.right}")
                logger.info(f"🔔 {contract_id}: Signal changed {old_signal} → {signal} with {confidence:.2f} confidence @ ₹{contract.ltp}")
                
        except Exception as e:
            contract_id = getattr(contract, 'short_hand', str(contract))
            logger.error(f"Error analyzing {contract_id}: {str(e)}")
    
    # Main loop
    while not stop_event.is_set():
        try:
            # Create a thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all contracts for processing
                executor.map(process_contract, contracts)
            
            # Sleep before next update
            sleep(update_interval)
            
        except Exception as e:
            logger.error(f"Error in signal generator: {str(e)}")
            sleep(update_interval)
    
    logger.info("Signal generator stopped")

def auto_trade_signals(contracts, quantity, confidence_threshold=0.5, candle_interval="1second", 
                      min_price=100, max_price=150, stop_loss=0.04, take_profit=0.05, stop_event=None):
    # Automatically trade based on signals generated for a list of contracts.
    
    # Create stop event if not provided
    if stop_event is None:
        stop_event = threading.Event()

    # Signal generator stop event
    signal_stop_event = threading.Event()

    # Flag to indicate if we're currently monitoring a position
    currently_monitoring = threading.Event()

    # Subscribe to feeds for all contracts
    logger.info(f"🔄 Subscribing to feeds for {len(contracts)} contracts")
    subscribe_multiple_feeds(contracts)
    subscribe_multiple_feeds(contracts, interval=candle_interval)

    # Start signal generator in a separate thread
    signal_thread = threading.Thread(
        target=run_signal_generator,
        args=(contracts, signal_stop_event, 20, 1, 10)  # min_candles=20, update_interval=1s, max_workers=10
    )
    signal_thread.daemon = True
    signal_thread.start()
    logger.info("✅ Signal generator started")
    logger.info(f"✅ Trading contracts with price between ₹{min_price} and ₹{max_price}")
    logger.info(f"✅ Using stop-loss: {stop_loss*100}%, take-profit: {take_profit*100}%")

    # Function to monitor a single position until signal changes
    def monitor_position(contract):
        try:
            # Set flag that we're monitoring a position
            currently_monitoring.set()
            
            entry_price = contract.ltp
            entry_time = datetime.now()
            
            logger.info(f"🔍 Monitoring position for {contract.short_hand} entered at ₹{entry_price}")
            
            while not stop_event.is_set():
                # Exit conditions
                '''if contract.signal != "BUY" and contract.confidence >= confidence_threshold:
                    logger.info(f"📊 Signal changed to {contract.signal} with confidence {contract.confidence:.2f}")
                    logger.info(f"🔄 Exiting position for {contract.short_hand} at ₹{contract.ltp}")
                    
                    # Place exit order
                    place_fno_order(contract, "sell", quantity)
                    break'''
                
                # Safety net: Set a stop-loss or take-profi1
                if contract.ltp is not None and entry_price is not None:
                    # Calculate current P&L percentage
                    current_pnl_percent = ((contract.ltp / entry_price) - 1) * 100
                    
                    # Stop-loss: Exit if loss exceeds stop-loss percentage
                    if current_pnl_percent < -stop_loss * 100:
                        logger.info(f"⚠️ Stop-loss triggered for {contract.short_hand} at ₹{contract.ltp}")
                        place_fno_order(contract, "sell", quantity)
                        break
                    
                    # Take-profit: Exit if profit exceeds take-profit percentage
                    if current_pnl_percent > take_profit * 100:
                        logger.info(f"💹 Take-profit triggered for {contract.short_hand} at ₹{contract.ltp}")
                        place_fno_order(contract, "sell", quantity)
                        break
                
                # Exit if position held for more than 30 minutes
                time_held = datetime.now() - entry_time
                if time_held.total_seconds() > 1800:  # 30 minutes in seconds
                    logger.info(f"⏱️ Max holding time reached for {contract.short_hand}, exiting at ₹{contract.ltp}")
                    place_fno_order(contract, "sell", quantity)
                    break
                
                # If price moves out of our range during monitoring, exit the position
                if contract.ltp < min_price or contract.ltp > max_price:
                    logger.info(f"⚠️ Price moved outside trading range ({min_price}-{max_price}) for {contract.short_hand}, exiting at ₹{contract.ltp}")
                    place_fno_order(contract, "sell", quantity)
                    break
                
                # Sleep to avoid excessive CPU usage
                sleep(1)
                
        except Exception as e:
            logger.error(f"❌ Error monitoring position for {contract.short_hand}: {e}")
            
            # Attempt emergency exit if there was an error
            try:
                logger.info(f"🔄 Emergency exit for {contract.short_hand}")
                place_fno_order(contract, "sell", quantity)
            except:
                logger.error(f"❌ Failed emergency exit for {contract.short_hand}")
        finally:
            # Clear the monitoring flag so we can move to the next contract
            currently_monitoring.clear()

    # Main trading loop
    try:
        logger.info("🚀 Starting trading system - one contract at a time")
        
        while not stop_event.is_set():
            # Only look for a new contract if we're not currently monitoring one
            if not currently_monitoring.is_set():
                # Look for the contract with the highest buy confidence
                best_contract = None
                best_confidence = confidence_threshold  # Minimum threshold
                
                for contract in contracts:
                    # Skip if no LTP or signal available
                    if not hasattr(contract, 'ltp') or contract.ltp is None or not hasattr(contract, 'signal'):
                        continue
                    
                    # Skip if price is outside our trading range
                    if contract.ltp < min_price or contract.ltp > max_price:
                        continue
                    
                    # Check if this is a strong buy signal
                    if contract.signal == "BUY" and hasattr(contract, 'confidence'):
                        if contract.confidence > best_confidence:
                            best_contract = contract
                            best_confidence = contract.confidence
                
                # If we found a good candidate, trade it
                if best_contract is not None:
                    logger.info(f"🔔 Found BUY signal for {best_contract.short_hand} with confidence {best_confidence:.2f} at price ₹{best_contract.ltp}")
                    
                    # Place buy order
                    response = place_fno_order(best_contract, "buy", quantity)
                    
                    # Check if order was successful
                    if response and response.get("Status") == 200:
                        logger.info(f"✅ Buy order placed for {best_contract.short_hand} at ₹{best_contract.ltp}")
                        
                        # Start monitoring thread for this position
                        monitor_thread = threading.Thread(
                            target=monitor_position,
                            args=(best_contract,)
                        )
                        monitor_thread.daemon = True
                        monitor_thread.start()
                    else:
                        logger.error(f"❌ Failed to place buy order for {best_contract.short_hand}")
                else:
                    # Log status periodically
                    candidate_count = sum(1 for c in contracts 
                                      if hasattr(c, 'ltp') and c.ltp is not None 
                                      and min_price <= c.ltp <= max_price)
                    
                    if candidate_count > 0:
                        logger.info(f"ℹ️ No strong BUY signals found among {candidate_count} contracts in price range ₹{min_price}-₹{max_price}")
            
            # Sleep to avoid excessive polling
            sleep(2)
            
    except KeyboardInterrupt:
        logger.info("👋 Trading system stopped by user")
    except Exception as e:
        logger.error(f"❌ Error in trading system: {e}")
    finally:
        # Cleanup
        try:
            # Stop signal generator
            signal_stop_event.set()
            
            # If we're currently monitoring a position, try to close it
            if currently_monitoring.is_set():
                for contract in contracts:
                    if hasattr(contract, 'ltp') and contract.ltp is not None:
                        logger.info(f"🔄 Attempting to close any open position for {contract.short_hand}")
                        place_fno_order(contract, "sell", quantity)
            
            # Unsubscribe from all feeds
            logger.info(f"🔄 Unsubscribing from feeds for {len(contracts)} contracts")
            unsubscribe_multiple_feeds(contracts)
            unsubscribe_multiple_feeds(contracts, interval=candle_interval)
            
            logger.info("✅ Trading system shutdown complete")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
   
def get_pnl(start_date = get_iso_date(), end_date = get_iso_date()):
    # Get trade book and calculate P&L
    all_trades = []
    
    # Get trades for NFO and BFO
    for exchange_code in ["NFO", "BFO"]:
        response = breeze.get_trade_list(
            exchange_code=exchange_code,
            from_date=start_date,
            to_date=end_date
        )
        
        if response.get("Status") == 200 and response.get("Success"):
            all_trades.extend(response.get("Success", []))
            print(f"Retrieved {int(len(response.get('Success', []))/2)} trades for {exchange_code}")
    
    if not all_trades:
        print("No trades found for the specified date range")
        return
    
    # Convert to DataFrame and calculate P&L
    trades_df = pd.DataFrame(all_trades)
    for col in ['quantity', 'average_cost', 'brokerage_amount', 'total_taxes']:
        trades_df[col] = pd.to_numeric(trades_df[col], errors='coerce')
    
    trades_df['trade_value'] = trades_df['quantity'] * trades_df['average_cost']
    trades_df['raw_pnl'] = trades_df.apply(
        lambda row: row['trade_value'] if row['action'] == 'Sell' else -row['trade_value'], 
        axis=1
    )
    trades_df['total_costs'] = trades_df['brokerage_amount'] + trades_df['total_taxes']
    trades_df['final_pnl'] = trades_df['raw_pnl'] - trades_df['total_costs']
    
    # Group by contract
    trades_df['contract_id'] = trades_df.apply(
        lambda row: f"{row['stock_code']}_{row['expiry_date']}_{row['strike_price']}_{row['right']}", 
        axis=1
    )
    
    contract_summary = trades_df.groupby(['exchange_code', 'contract_id']).agg({
        'stock_code': 'first',
        'expiry_date': 'first',
        'strike_price': 'first',
        'right': 'first',
        'final_pnl': 'sum',
    }).reset_index()
    
    # Print contract-wise P&L by exchange
    print("\n===== CONTRACT-WISE P&L =====")
    
    # First NFO contracts
    print("\nNFO CONTRACTS:")
    nfo_contracts = contract_summary[contract_summary['exchange_code'] == 'NFO'].sort_values('final_pnl', ascending=False)
    for _, row in nfo_contracts.iterrows():
        print(f"{row['stock_code']} {row['expiry_date']} {row['strike_price']} {row['right']}: ₹ {row['final_pnl']:,.2f}")
    
    # Then BFO contracts
    print("\nBFO CONTRACTS:")
    bfo_contracts = contract_summary[contract_summary['exchange_code'] == 'BFO'].sort_values('final_pnl', ascending=False)
    for _, row in bfo_contracts.iterrows():
        print(f"{row['stock_code']} {row['expiry_date']} {row['strike_price']} {row['right']}: ₹ {row['final_pnl']:,.2f}")
    
    # Calculate totals
    gross_pnl = trades_df['raw_pnl'].sum()
    brokerage = trades_df['brokerage_amount'].sum()
    taxes = trades_df['total_taxes'].sum()
    net_pnl = gross_pnl - brokerage - taxes
    
    # Estimate tax components
    stt = taxes * 0.65
    transaction = taxes * 0.15
    stamp = taxes * 0.10
    gst = brokerage * 0.18
    
    # Print overall summary
    print("\n===== TRADE P&L SUMMARY =====")
    print(f"Total Trades: {int(len(trades_df)/2)}")
    print(f"Gross P&L (Before Charges): ₹ {gross_pnl:,.2f}")
    print("\nCHARGES BREAKDOWN:")
    print(f"Brokerage: ₹{brokerage:,.2f}")
    print(f"STT (est.): ₹{stt:,.2f}")
    print(f"Transaction Charges (est.): ₹ {transaction:,.2f}")
    print(f"Stamp Duty (est.): ₹{stamp:,.2f}")
    print(f"GST (est.): ₹{gst:,.2f}")
    print(f"Total Charges: ₹{(brokerage + taxes):,.2f}")
    print("\nNET P&L (After Charges): ₹{:,.2f}".format(net_pnl))
    
# Main execution
if __name__ == "__main__":
    # Initiate logger
    logger = log()
    logger.info(f"✅ Current IST time: {get_ist_time()}")
    
    # Load the token mapping csv
    load_token_mapping()
    
    # Prompt session token
    session_token = input("Enter session token: ")
    
    # Initialize BreezeConnect
    breeze = connect()

    # Give kill alias to az5
    az5 = kill = az5
