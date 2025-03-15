from breeze_connect import BreezeConnect
from datetime import datetime, timedelta, timezone
import logging
import pytz
from IPython.display import clear_output
import os
import threading
from time import sleep
from concurrent.futures import ThreadPoolExecutor

# Global Variables
breeze = None
logger = None
contract_registry = {}

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
        if logger:
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
        if logger:
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
            try:
                if ticks.get("last"):
                    contract_key = None
                    try:
                        # Use the utility function to get ISO date
                        expiry_date = get_iso_date(ticks.get("expiry_date")).split('T')[0]
                        
                        if ticks.get("product_type") == "Options":
                            contract_key = f"{ticks.get('stock_code')}-{expiry_date}-{ticks.get('strike_price')}-{ticks.get('right').lower()}"
                        else:
                            contract_key = f"{ticks.get('stock_code')}-FUT-{expiry_date}"
                        
                        if contract_key and contract_key in contract_registry:
                            contract = contract_registry[contract_key]
                            contract.ltp = float(ticks["last"])
                            contract.last_update_time = datetime.now()
                    except Exception as e:
                        # Silent handling for expected parse errors
                        pass
                    
            except Exception as e:
                logger.error(f"❌ Error in websocket callback: {e}")
        
        breeze.on_ticks = on_ticks
        breeze.ws_connect()
        logger.info("✅ Websocket connected")
        return breeze
        
    except Exception as e:
        logger.error(f"❌ Failed to generate session: {e}")
        exit(1)

def subscribe_feed(contract):
    #Subscribe to real-time feed for a contract
    global breeze, logger
    
    if contract.is_subscribed:
        logger.info(f"ℹ️ {contract.shorthand} is already subscribed")
        return
        
    try:
        # Convert ISO date to Breeze format using utility function
        expiry_date = convert_iso_to_breeze_date(contract.expiry_date)
        
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
        
        # Register this contract so the callback can find it
        contract_registry[contract.registry_key] = contract
            
        contract.is_subscribed = True
        logger.info(f"✅ Subscribed to feeds for {contract.shorthand}")
        
    except Exception as e:
        logger.error(f"❌ Failed to subscribe feeds for {contract.shorthand}: {e}")
        raise

def unsubscribe_feed(contract):
    #Unsubscribe from real-time feed for a contract
    global breeze, logger
    
    if not contract.is_subscribed:
        logger.info(f"ℹ️ {contract.shorthand} is not subscribed")
        return
        
    try:
        # Convert ISO date to Breeze format using utility function
        expiry_date = convert_iso_to_breeze_date(contract.expiry_date)
        
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
        
        # Unregister this contract
        if contract.registry_key in contract_registry:
            del contract_registry[contract.registry_key]
        
        contract.is_subscribed = False
        logger.info(f"✅ Unsubscribed from feeds for {contract.shorthand}")
        
    except Exception as e:
        logger.error(f"❌ Failed to unsubscribe feeds for {contract.shorthand}: {e}")
        raise

def subscribe_multiple_feeds(contracts):
    #Subscribe to multiple feeds at once
    for contract in contracts:
        subscribe_feed(contract)

def unsubscribe_multiple_feeds(contracts):
    #Unsubscribe from multiple feeds at once
    for contract in contracts:
        unsubscribe_feed(contract)

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
    def __init__(self, stock_code, exchange_code, product_type, expiry_date, right="others", strike_price="0", auto_subscribe=False):
        self.stock_code = stock_code
        self.exchange_code = exchange_code
        self.product_type = product_type
        self.expiry_date = expiry_date
        self.right = right
        self.strike_price = strike_price
        
        # Extract date part for consistency
        date_part = expiry_date.split('T')[0]
        
        if product_type == "options":
            self.shorthand = f"{stock_code}-{date_part}-{strike_price}-{right}"
            self.registry_key = self.shorthand
        else:
            self.shorthand = f"{stock_code}-FUT-{date_part}"
            self.registry_key = self.shorthand
        
        # Add fields for live price tracking
        self.ltp = None
        self.last_update_time = None
        self.is_subscribed = False
            
        # Auto-subscribe if requested
        if auto_subscribe:
            subscribe_feed(self)
        
def generate_contracts(stock_code, expiry_date, start_strike, end_strike, interval, keyword=None, exchange_code="NFO", product_type="options", auto_subscribe=False):
    # Generate call and put option contracts for a range of strike prices
    contracts = []
    
    for strike in range(start_strike, end_strike+1, interval):
        # Generating calls
        call_contract = contract(stock_code, exchange_code, product_type, expiry_date, "call", strike, auto_subscribe)
        if keyword:
            globals()[f"{stock_code}{strike}CE{keyword}"] = call_contract
        else:
            globals()[f"{stock_code}{strike}CE"] = call_contract
        contracts.append(call_contract)
    
        # Generating puts
        put_contract = contract(stock_code, exchange_code, product_type, expiry_date, "put", strike, auto_subscribe)
        if keyword:
            globals()[f"{stock_code}{strike}PE{keyword}"] = put_contract
        else:
            globals()[f"{stock_code}{strike}PE"] = put_contract
        contracts.append(put_contract)
        
        print(f"ℹ️ {stock_code}{strike}CE{keyword if keyword else ''}, {stock_code}{strike}PE{keyword if keyword else ''} generated.")
    
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
                logger.info(f"✅ {contract.shorthand}-{action} order successful.")
            else:
                logger.error(f"❌ {contract.shorthand}-{action} order failed.")
                if response.get("Error") is not None:
                    logger.error(f"❌ Error details: {response.get('Error')}")
            return response
        except Exception as e:
            logger.error(f"❌ Error in placing order:{contract.shorthand}-{action} {e}")
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
            logger.error(f"❌ API error in fetching {contract.shorthand}")
            print(response)
            return None
    except Exception as e:
        logger.error(f"❌ Error fetching {contract.shorthand} price: {e}")
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
        wait_count = 0
        while sell_contract.ltp is None:
            sleep(0.1)
            wait_count += 1
            if wait_count > 100:  # Timeout after ~10 seconds
                logger.warning(f"⚠️ No price data received after 10s for {sell_contract.shorthand}")
                if threshold == 0:
                    # Try to get initial price via API if WebSocket doesn't deliver
                    threshold = get_price(sell_contract)
                    if threshold is None:
                        logger.error(f"❌ Could not get price for {sell_contract.shorthand}")
                        return
                break
           
        # Get initial price if threshold not specified
        if threshold == 0:
            threshold = sell_contract.ltp or get_price(sell_contract)
            logger.info(f"✅ Using threshold: {threshold}")

        # Place initial buy order if specified
        if buy_contract and buy_quantity and buy_multiple:
            place_fno_order(buy_contract, "buy", buy_quantity, buy_multiple)

        # Main trading loop
        while running:
            price = sell_contract.ltp
            iterations += 1
           
            if iterations % 10 == 0:
                clear_output(wait=True)
                print(f"Threshold: {threshold} | Price: {price} | Count: {count} | Position: {position} | Iterations: {iterations}")
           
            if price is not None:
                if position == 0 and price < threshold-1:
                    place_fno_order(sell_contract, "sell", sell_quantity, sell_multiple)
                    position = -1
                    count += 1
                    logger.info(f"✅ Sold at {price}")
                   
                elif position == -1 and price > threshold:
                    place_fno_order(sell_contract, "buy", sell_quantity, sell_multiple)
                    position = 0
                    logger.info(f"✅ Bought at {price}")
               
            sleep(0.2)
               
    except KeyboardInterrupt:
        logger.info("Operation terminated by user.")
    except Exception as e:
        logger.error(f"❌ Error in main loop: {e}")
    finally:
        running = False
        unsubscribe_feed(sell_contract)

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
        last_updates = {contract.shorthand: {"price": None, "time": None} for contract in contracts}
        
        while True:
            clear()
            now = datetime.now()
            
            # Print header
            print(f"{'Contract':<25} {'Price':<10} {'Last Update':<15}")
            print("-" * 55)
            
            # Update and show data for each contract
            for contract in contracts:
                last_data = last_updates[contract.shorthand]
                
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
                print(f"{contract.shorthand:<25} {contract.ltp if contract.ltp else 'No data':<10} {time_since_update:<15}")
            
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

# Main execution
if __name__ == "__main__":
    # Prompt session token
    session_token = input("Enter session token: ")
    
    # Initiate logger
    logger = log()
    logger.info(f"✅ Current IST time: {get_ist_time()}")
    
    # Initialize BreezeConnect
    breeze = connect()

    # Give kill alias to az5
    az5 = kill = az5
