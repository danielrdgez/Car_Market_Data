import logging
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
from selenium_stealth import stealth
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from datetime import date, datetime, timezone
import os
import urllib3
import sys
import json

def setup_driver(headless=True):
    """Initializes a stealth-configured Chrome driver."""
    options = webdriver.ChromeOptions()
    
    # CRITICAL: Disable internal automation flags and logging
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-popup-blocking')
    options.page_load_strategy = 'eager'
    
    # Enable Performance Logging for API Interception
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    
    if headless:
        options.add_argument('--headless=new')
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(options=options)

    # Apply Stealth Library configurations
    stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
    )
    return driver

output_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT")

# Create the output directory if it doesn't exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Initialize the Chrome driver with stealth options
driver = setup_driver(headless=True)
driver.set_page_load_timeout(60)
driver.implicitly_wait(0)  # Use explicit waits for better control and reliability

# Set up logging
logging.basicConfig(
    filename=os.path.join(output_directory, f'scraping_{date.today()}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#input_make = "hyundai"
#input_model = "veloster"
input_zip = "33186"
input_radius = "50"
input_year = 2000
input_state = "country"

driver.get(f'https://www.autotempest.com/results?localization={input_state}&zip={input_zip}')

continue_buttons_xpath = {
    "autotempest" : '//*[@id="te-results"]/section/button',
    "hemmings" : '//*[@id="hem-results"]/section/button',
    "cars" : '//*[@id="cm-results"]/section/button',
    "carsoup" : '//*[@id="cs-results"]/section/button',
    "carvana" : '//*[@id="cv-results"]/section/button',
    "ebay" : '//*[@id="eb-results"]/section/button',
    "other" : '//*[@id="ot-results"]/section/button'
}
    
def wait_for_page_load(timeout=10):
    """Wait for page to complete loading."""
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        return True
    except TimeoutException:
        logging.warning('Timeout waiting for page load')
        return False

def click_button(xpath, timeout=30):
    """Clicks a button with robust fallbacks and randomized waits."""
    try:
        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(random.uniform(0.6, 1.2))  # Human-like pause
        
        try:
            button.click()
        except:
            try:
                ActionChains(driver).move_to_element(button).click().perform()
            except:
                driver.execute_script("arguments[0].click();", button)
        
        if wait_for_page_load(timeout):
            logging.info("New content loaded successfully")
            return True
        else:
            logging.info("No new content detected after click")
            return False
            
    except Exception as e:
        print(f"Failed to click button: {e}")
        return False

def handle_ribbon():
    """Handles the potential appearance of a dismissible ribbon."""
    try:
        WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
        )
        
        try:
            dismiss_button = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="cta-dismiss"]/i'))
            )
            driver.execute_script("arguments[0].scrollIntoView(true);", dismiss_button)
            time.sleep(random.uniform(0.8, 1.5)) # Human-like pause
            ActionChains(driver).move_to_element(dismiss_button).click().perform()
            
            WebDriverWait(driver, 2).until(
                EC.invisibility_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
            )
        except Exception as e:
            print(f"Failed to dismiss ribbon: {e}")
            return False
            
        return True
    except (NoSuchElementException, TimeoutException):
        return False

def parse_performance_logs_for_queue_results(seen_request_ids):
    """Extracts car data from 'queue-results' API responses in performance logs."""
    rows = []
    try:
        logs = driver.get_log("performance")
        for entry in logs:
            try:
                msg = json.loads(entry["message"])['message']
            except Exception:
                continue
            if msg.get("method") != "Network.responseReceived":
                continue
            params = msg.get("params", {})
            response = params.get("response", {})
            url = response.get("url", "")
            if "queue-results" not in url:
                continue
            request_id = params.get("requestId")
            if not request_id or request_id in seen_request_ids:
                continue
            seen_request_ids.add(request_id)
            try:
                body = driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                text = body.get("body", "")
                if not text:
                    continue
                api_json = json.loads(text)
                rows.extend(extract_rows_from_api_data(api_json))
                print(f"Captured queue-results response (requestId={request_id})")
            except WebDriverException:
                continue
    except Exception as e:
        print(f"Error reading performance logs: {e}")
    return rows

def extract_rows_from_api_data(api_data):
    """Extracts and formats car data from the API JSON response."""
    rows = []
    items = api_data.get("items") or api_data.get("results") or []
    
    fields_to_extract = [
        "date", "location", "locationCode", "countryCode",
        "pendingSale", "title", "currentBid", "bids", "distance", "priceHistory",
        "priceRecentChange", "price", "listingHistory", "mileage", "year", "vin", "sellerType", "vehicleTitle", "listingType",
        "vehicleTitleDesc", "sourceName", "img", "externalID"
    ]
    
    for item in items:
        row = {
            "loaddate": date.today().isoformat()
        }
        
        # Concatenate details columns
        details_short = item.get("detailsShort") or ""
        details_mid = item.get("detailsMid") or ""
        details_long = item.get("detailsLong") or ""
        row["details"] = f"{details_short}{details_mid}{details_long}"

        for field in fields_to_extract:
            value = item.get(field)
            if isinstance(value, (list, dict)):
                row[field] = json.dumps(value, ensure_ascii=False)
            else:
                row[field] = value
        rows.append(row)
    return rows

def append_api_rows_to_csv(rows, seen_vins, csv_path):
    """Appends new rows to the CSV, avoiding duplicates."""
    if not rows:
        return 0
    
    new_rows = [r for r in rows if r.get("vin") and r.get("vin") not in seen_vins]
    if not new_rows:
        return 0
        
    for r in new_rows:
        seen_vins.add(r.get("vin"))

    df = pd.DataFrame(new_rows)
    file_exists = os.path.exists(csv_path)
    
    try:
        df.to_csv(csv_path, mode='a', header=not file_exists, index=False)
        print(f"Appended {len(df)} new rows to CAR_DATA CSV")
        return len(df)
    except Exception as e:
        print(f"Error appending to CSV: {e}")
        return 0

def car_data():
    """Main function to drive the data collection process."""
    max_retries = 3
    iteration = 1
    unclickable_buttons = set()
    seen_api_request_ids = set()
    
    csv_path = os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv")
    seen_vins = set()
    if os.path.exists(csv_path):
        try:
            print("Loading existing VINs to avoid duplicates...")
            existing_data = pd.read_csv(csv_path, usecols=["vin"], on_bad_lines='skip')
            seen_vins = set(existing_data["vin"].dropna().astype(str).tolist())
            print(f"Loaded {len(seen_vins)} existing VINs")
        except Exception as e:
            print(f"Error loading existing VINs: {e}")
    
    try:
        driver.execute_cdp_cmd("Network.enable", {})
    except Exception as e:
        print(f"Warning: Could not enable Network CDP command: {e}")
    
    try:
        while True:
            loop_start_time = time.time()
            print(f"\nStarting iteration {iteration}")
            
            any_button_clicked = False
            
            for button_name, button_xpath in continue_buttons_xpath.items():
                if button_name in unclickable_buttons:
                    continue
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        print(f"Attempting to click '{button_name}' button...")
                        
                        if handle_ribbon():
                            print("Ribbon found and dismissed")
                        
                        if click_button(button_xpath):
                            print(f"Successfully clicked '{button_name}' button")
                            any_button_clicked = True
                            break
                        else:
                            print(f"Button '{button_name}' not found or not clickable, adding to skip list.")
                            unclickable_buttons.add(button_name)
                            break
                        
                    except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                        print(f"\nConnection error during button click: {e}. Exiting.")
                        driver.quit()
                        sys.exit(1)
                    except Exception as e:
                        retry_count += 1
                        print(f"Error clicking '{button_name}' button (attempt {retry_count}): {e}")
                        if retry_count >= max_retries:
                            print(f"Failed to click '{button_name}' after {max_retries} attempts.")
            
            try:
                api_rows = parse_performance_logs_for_queue_results(seen_api_request_ids)
                if api_rows:
                    append_api_rows_to_csv(api_rows, seen_vins, csv_path)
            except Exception as e:
                print(f"Warning: Error collecting API data: {e}")
            
            if not any_button_clicked:
                print("\nNo more active 'load more' buttons found. Finishing...")
                break
            
            loop_duration = time.time() - loop_start_time
            duration_msg = f"Iteration {iteration} took {loop_duration:.2f} seconds"
            print(duration_msg)
            logging.info(duration_msg)
            
            iteration += 1

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        print(f"\nData collection finished. Output saved to {csv_path}")

if __name__ == "__main__":
    car_data()
