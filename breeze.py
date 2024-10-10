from breeze_connect import BreezeConnect
from datetime import datetime, timedelta, timezone
import logging
import pytz
from IPython.display import clear_output
import os

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
    # Load environment variables
    api_key = os.environ.get("BREEZE_API_KEY")
    api_secret = os.environ.get("BREEZE_API_SECRET_KEY")
    # Initialize BreezeConnect
    breeze = BreezeConnect(api_key)
    
    # Generate Session
    try:
        breeze.generate_session(api_secret, session_token)
        logger.info("Successfully connected to BreezeConnect")
        return breeze
    except Exception as e:
        logger.error(f"Failed to generate session: {e}")
        exit(1)
        
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
        
def generate_contarcts(stock_code, expiry_date, start_strike, end_strike, interval, exchange_code = "NFO", product_type = "options"):
    for strike in range(start_strike, end_strike+1, interval) :
        # Generating calls
        globals()[f"{stock_code}{strike}CE"] = contract(stock_code, exchange_code, product_type, expiry_date, "call", strike)
    
        # Genrating puts
        globals()[f"{stock_code}{strike}PE"] = contract(stock_code, exchange_code, product_type, expiry_date, "put", strike)
        print(f"{stock_code}{strike}CE, {stock_code}{strike}PE generated.")
        
def clear():
    clear_output(wait=True)    

def place_fno_order(contract, action, quantity, split = 0, price="0"):
    try:
        i = 0
        while i < split:
            response = breeze.place_order(
                stock_code=contract.stock_code,
                exchange_code=contract.exchange_code,
                product=contract.product_type,
                action=action,
                order_type = ("market" if price == "0" else "limit"),
                stoploss="",
                quantity=quantity/split,
                price=price,
                validity="day",
                validity_date=current_date,
                disclosed_quantity="0",
                expiry_date=contract.expiry_date,
                right=contract.right,
                strike_price=contract.strike_price
            )
            if response.get("Status") == 200:
                logger.info(f"{contract.shorthand}-{action} order successful.")
            else:
                logger.error(f"{contract.shorthand}-{action} order failed.")
            i += 1
                
    except Exception as e:
        logger.error(f"Error in placing order: {e}")
    return response

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

def place_hedge_order(contract1, contract2, quantity, split = 1):
    try:
        i = 0
        while i < split:
            response = place_fno_order(contract1, "buy", (quantity/split), price="0")
            if response.get("Status") == 200 :
                response = place_fno_order(contract2, "sell", (quantity/split), price="0")
                while response.get("Status") != 200 :
                    logger.info(f"Retrying...")
                    response = place_fno_order(contract2, "sell", (quantity/split), price="0")
            else:
                logger.info(f"Retrying...")
                i -= 1
            i += 1

    except KeyboardInterrupt:
        logger.info("Program terminated by user.")
    except Exception as e:
        logger.error(f"Unexpected error in loop: {e}")

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

    
