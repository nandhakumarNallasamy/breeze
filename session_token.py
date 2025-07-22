import os
import time
import urllib.parse
import re
from time import sleep
import pyotp

# Selenium imports
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

def get_google_authenticator_otp():
    # Generate OTP using Google Authenticator secret"""
    google_auth_secret = os.environ.get("GOOGLE_AUTH_SECRET")
    
    try:
        # Create TOTP object
        totp = pyotp.TOTP(google_auth_secret)
        
        # Generate current OTP
        otp = totp.now()
        
        print(f"✅ Generated OTP: {otp}")
        return otp
        
    except Exception as e:
        print(f"❌ Error generating OTP: {e}")
        return None

def get_icici_session_token():
    # Retrieve session token by logging into ICICI Direct with Google Authenticator OTP"""
    # Retrieve credentials
    api_key = os.environ.get("BREEZE_API_KEY")
    api_secret = os.environ.get("BREEZE_API_SECRET_KEY")
    username = os.environ.get("ICICI_DIRECT_USERNAME")
    password = os.environ.get("ICICI_DIRECT_PASSWORD")
    
    # Validate credentials
    if not all([api_key, api_secret, username, password]):
        print("❌ Error: Missing credentials")
        print("Required environment variables:")
        print("- BREEZE_API_KEY")
        print("- BREEZE_API_SECRET_KEY") 
        print("- ICICI_DIRECT_USERNAME")
        print("- ICICI_DIRECT_PASSWORD")
        print("- GOOGLE_AUTH_SECRET")
        return None
    
    # Configure Chrome WebDriver with optimized options for speed
    chrome_options = Options()
    
    # Performance optimizations
    chrome_options.add_argument("--headless")                    # No GUI - much faster
    chrome_options.add_argument("--no-sandbox")                 # Bypass OS security model
    chrome_options.add_argument("--disable-dev-shm-usage")      # Overcome limited resource problems
    chrome_options.add_argument("--disable-gpu")                # Disable GPU acceleration
    chrome_options.add_argument("--disable-web-security")       # Disable web security
    chrome_options.add_argument("--disable-extensions")         # Disable extensions
    chrome_options.add_argument("--disable-plugins")            # Disable plugins
    chrome_options.add_argument("--disable-images")             # Don't load images - saves bandwidth
    chrome_options.add_argument("--disable-javascript")         # Disable JS if possible (may break login)
    chrome_options.add_argument("--no-first-run")               # Skip first-run setup
    chrome_options.add_argument("--disable-default-apps")       # Disable default apps
    chrome_options.add_argument("--disable-background-timer-throttling")  # Don't throttle background tabs
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-features=TranslateUI")          # Disable translate
    chrome_options.add_argument("--disable-ipc-flooding-protection")      # Speed up IPC
    
    # Memory optimizations
    chrome_options.add_argument("--memory-pressure-off")        # Don't limit memory
    chrome_options.add_argument("--max_old_space_size=4096")     # Increase memory limit
    
    # Network optimizations
    chrome_options.add_argument("--aggressive-cache-discard")   # Discard cache aggressively
    chrome_options.add_argument("--disable-background-networking")  # Disable background network requests
    
    # Set page load strategy for faster loading
    chrome_options.add_argument("--page-load-strategy=eager")   # Don't wait for all resources
    
    # User agent (some sites block headless browsers)
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Window size (even in headless mode)
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Prefs for additional optimizations
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,                    # Block images
            "plugins": 2,                   # Block plugins
            "popups": 2,                    # Block popups
            "geolocation": 2,               # Block location sharing
            "notifications": 2,             # Block notifications
            "media_stream": 2,              # Block media stream
        },
        "profile.managed_default_content_settings": {
            "images": 2                     # Block images
        }
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Disable logging for cleaner output
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument('--disable-logging')
    chrome_options.add_argument('--log-level=3')  # Suppress INFO, WARNING, ERROR
    chrome_options.add_argument('--silent')
    
    driver = None
    try:
        print("🚀 Starting optimized browser session...")
        # Setup WebDriver with faster timeout
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Set shorter timeouts for faster failure
        driver.set_page_load_timeout(10)  
        driver.implicitly_wait(5)         
        
        # Generate login URL
        encoded_api_key = urllib.parse.quote_plus(api_key)
        login_url = f"https://api.icicidirect.com/apiuser/login?api_key={encoded_api_key}"
        
        print(f"🔗 Navigating to login URL")
        driver.get(login_url)
        sleep(1.5)  
        
        # Find and fill user ID
        try:
            user_id_input = WebDriverWait(driver, 6).until( 
                EC.presence_of_element_located((By.ID, "txtuid"))
            )
            user_id_input.clear()
            user_id_input.send_keys(username)
            
            # Find and fill password
            password_input = driver.find_element(By.ID, "txtPass")
            password_input.clear()
            password_input.send_keys(password)
            
            # Check terms and conditions
            terms_checkbox = driver.find_element(By.ID, "chkssTnc")
            if not terms_checkbox.is_selected():
                terms_checkbox.click()
            
            # Click login button
            login_button = driver.find_element(By.ID, "btnSubmit")
            driver.execute_script("arguments[0].click();", login_button)
            sleep(2.5)  
            
        except Exception as e:
            print(f"❌ Error during initial login: {e}")
            return None
        
        # Check for OTP verification page
        current_url = driver.current_url
        print(f"📍 Current URL: {current_url}")
        
        # Check if we're on the OTP verification page
        if "tradelogin" in current_url.lower() or "Verify OTP" in driver.page_source:
            print("🔑 OTP verification page detected")
            
            # Generate OTP using Google Authenticator
            print("🔐 Generating OTP using Google Authenticator...")
            otp = get_google_authenticator_otp()
            
            if not otp:
                print("❌ Failed to generate OTP")
                return None
            
            print(f"✅ OTP generated: {otp}")
            
            # Find all input fields
            try:
                # Find visible text input fields
                input_fields = driver.find_elements(By.TAG_NAME, "input")
                text_inputs = []
                for inp in input_fields:
                    input_type = inp.get_attribute("type")
                    if input_type in ["text", "tel", "password", "number"] and inp.is_displayed():
                        text_inputs.append(inp)
                
                print(f"Found {len(text_inputs)} visible text input fields")
                
                # Enter OTP digits - specifically handling the 6 separate fields
                if len(text_inputs) >= 6:
                    print("Entering OTP digits into separate fields...")
                    
                    for i, digit in enumerate(otp[:6]):
                        if i < len(text_inputs):
                            # Use JavaScript to set value (more reliable)
                            driver.execute_script(f"arguments[0].value = '{digit}';", text_inputs[i])
                            driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", text_inputs[i])
                            driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", text_inputs[i])
                            print(f"✅ Entered digit {digit} in field {i+1}")
                            sleep(0.1) 
                
                    print("✅ All OTP digits entered")
                    sleep(0.5)  
                
                # Find and click Submit button
                print("🔍 Looking for Submit button...")
                submit_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Submit')] | //input[@value='Submit']")
                if submit_buttons:
                    print("✅ Found Submit button by text")
                    driver.execute_script("arguments[0].click();", submit_buttons[0])
                else:
                    # Try JavaScript to find and click the Submit button
                    script = """
                    var buttons = document.querySelectorAll('button, input[type="button"], input[type="submit"]');
                    for (var i = 0; i < buttons.length; i++) {
                        var text = (buttons[i].textContent || '').toLowerCase();
                        var value = (buttons[i].value || '').toLowerCase();
                        
                        if (text.includes('submit') || value.includes('submit')) {
                            buttons[i].click();
                            return true;
                        }
                    }
                    return false;
                    """
                    driver.execute_script(script)
                
                # Wait for potential redirect after submission
                print("⏳ Waiting for redirection...")
                sleep(2) 
                
            except Exception as e:
                print(f"❌ Error handling OTP: {e}")
                return None
        
        # Check for session token in URL
        current_url = driver.current_url
        print(f"📍 Final URL: {current_url}")
        
        # Check for various token formats in URL
        if "session_token=" in current_url:
            token_start = current_url.find("session_token=")
            session_token = current_url[token_start+len("session_token="):].split("&")[0]
            return session_token
        elif "apisession=" in current_url:
            token_start = current_url.find("apisession=")
            session_token = current_url[token_start+len("apisession="):].split("&")[0]
            return session_token
        elif "breezestream://" in current_url:
            # For custom protocol URLs
            matches = re.search(r'breezestream://\?apisession=([^&]+)', current_url)
            if matches:
                session_token = matches.group(1)
                return session_token
        
        # Check page source for token
        page_source = driver.page_source
        token_patterns = [
            r'session_token["\s:=]+([^"&\s]+)',
            r'token["\s:=]+([^"&\s]+)',
            r'authToken["\s:=]+([^"&\s]+)'
        ]
        
        for pattern in token_patterns:
            token_match = re.search(pattern, page_source)
            if token_match:
                return token_match.group(1)
        
        return None
    
    except Exception as e:
        print(f"❌ Authentication failed: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
                print("✅ Optimized browser session closed")
            except:
                pass

def main():
    try:
        session_token = get_icici_session_token()
        
        if session_token:
            print("\n===== SESSION TOKEN =====")
            print(session_token)
            print("✅ Session token successfully retrieved!")
            
            # Optional: Validate token with Breeze API
            try:
                # Import only when this main() function runs (not when module is imported)
                from breeze_connect import BreezeConnect
                
                api_key = os.environ.get("BREEZE_API_KEY")
                api_secret = os.environ.get("BREEZE_API_SECRET_KEY")
                breeze = BreezeConnect(api_key)
                breeze.generate_session(api_secret=api_secret, session_token=session_token)
                print("✅ Token validated with Breeze API")
            except Exception as e:
                print(f"⚠️ Token validation warning: {e}")
        else:
            print("❌ Failed to retrieve session token")
    
    except KeyboardInterrupt:
        print("\n👋 Operation cancelled by user.")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
