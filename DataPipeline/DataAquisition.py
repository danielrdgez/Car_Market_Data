import logging
from re import X
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains 
from datetime import date, datetime, timezone
import os
import urllib3
import sys
import json

output_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT")

# Create the output directory if it doesn't exist
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# Configure Chrome options
options = webdriver.ChromeOptions()
options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
options.add_argument('--disable-blink-features=AutomationControlled')
#options.add_argument('--start-maximized')
options.add_argument('--disable-popup-blocking')
options.add_argument('--headless')
#options.add_argument('--no-sandbox')
#options.add_argument('--disable-dev-shm-usage')
#options.add_argument('--disable-gpu')
#options.add_argument('--disable-browser-side-navigation')
#options.add_argument('--disable-infobars')
#options.add_argument('--disable-extensions')
options.page_load_strategy = 'eager'  # Load faster by not waiting for all resources
# Enable CDP performance logging for capturing network responses
options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

# Initialize the Chrome driver with options
driver = webdriver.Chrome(options=options)
driver.set_page_load_timeout(60)  # Set page load timeout
driver.implicitly_wait(2)  # Increase implicit wait time

# Set up logging
logging.basicConfig(
    filename=os.path.join(output_directory, f'scraping_{date.today()}.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#input_make = "hyundai"      #input(f'Make:{str()}').casefold()
#input_model = "veloster"        #input(f'Model:{str()}').casefold()
input_zip = "33186"      #input(f'Zip:{int(max=5)}')
input_radius = "50"       #input(f'Radius:{str(max=4)}')
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
    #"truecar" : '//*[@id="tc-results"]/section/button',
    "other" : '//*[@id="ot-results"]/section/button'
}
    
def wait_for_page_load(timeout=10):
    """Wait for page to complete loading using Selenium's built-in mechanisms"""
    try:
        # Wait for document ready state
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script('return document.readyState') == 'complete'
        )
        
        # Wait for specific element that indicates new content
        #WebDriverWait(driver, timeout).until(
        #    EC.presence_of_all_elements_located((By.CLASS_NAME, 'description-wrap'))
        #)
        
        return True
    except TimeoutException:
        logging.warning('Timeout waiting for page load')
        return False

def click_button(xpath, timeout=30):
    try:
        # Wait for page to be fully loaded before attempting to click
        #wait_for_page_load(timeout)
        
        # Wait for element to be clickable with increased timeout
        button = WebDriverWait(driver, timeout).until(
            EC.element_to_be_clickable((By.XPATH, xpath))
        )
        
        # Scroll element into view and add padding to ensure it's visible
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
        time.sleep(0.5)  # Increased wait time after scroll
        
        # Try to click the button in different ways
        try:
            # Try regular click first
            button.click()
        except:
            try:
                # Try ActionChains click
                ActionChains(driver).move_to_element(button).click().perform()
            except:
                # Try JavaScript click as last resort
                driver.execute_script("arguments[0].click();", button)
        
        # Wait for new content to load using improved wait mechanism
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
    try:
        # Check if the ribbon exists with a reasonable timeout
        ribbon = WebDriverWait(driver, 1).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
        )
        
        # If ribbon exists, try to find and click the dismiss button
        try:
            dismiss_button = WebDriverWait(driver, 1).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="cta-dismiss"]/i'))
            )
            # Scroll to the dismiss button
            driver.execute_script("arguments[0].scrollIntoView(true);", dismiss_button)
            driver.implicitly_wait(1)  # Small pause for stability
            ActionChains(driver).move_to_element(dismiss_button).click().perform()
            
            # Wait for ribbon to disappear
            WebDriverWait(driver, 1).until(
                EC.invisibility_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
            )
        except Exception as e:
            print(f"Failed to dismiss ribbon: {e}")
            return False
            
        return True
    except (NoSuchElementException, TimeoutException):
        return False  # Ribbon not found, which is fine

# ============= CDP-based JSON data extraction functions =============
def parse_performance_logs_for_queue_results(seen_request_ids):
    """Extract queue-results API responses from performance logs"""
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
            except Exception:
                continue
    except Exception as e:
        print(f"Error reading performance logs: {e}")
    return rows

def extract_rows_from_api_data(api_data):
    """Extract individual car rows from API JSON response with selected fields"""
    rows = []
    items = api_data.get("items") or api_data.get("results") or []
    
    # Fields to extract from each item
    fields_to_extract = [
        "date", "endDate", "archiveDate", "location", "locationCode", "countryCode",
        "pendingSale", "title", "trim", "currentBid", "bids", "distance", "priceHistory",
        "priceRecentChange", "price", "listingHistory", "mileage", "year", "vin", "sellerType", "vehicleTitle", "listingType",
        "vehicleTitleDesc", "sourceName", "detailsShort", "detailsMid", "detailsLong", "detailsExtraLong", "img"
    ]
    
    for item in items:
        row = {}
        row["timestamp"] = datetime.now(timezone.utc).isoformat()
        row["loaddate"] = date.today()
        
        # Extract only the specified fields
        for field in fields_to_extract:
            value = item.get(field)
            # Serialize nested structures (lists/dicts) to JSON strings
            if isinstance(value, (list, dict)):
                row[field] = json.dumps(value, ensure_ascii=False)
            else:
                row[field] = value
        
        rows.append(row)
    return rows

def append_api_rows_to_csv(rows, seen_vins=None, csv_path=None):
    """Append API-extracted rows to CAR_DATA CSV"""
    if not rows:
        return 0
    if csv_path is None:
        csv_path = os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv")
    
    # Filter out rows that have already been collected (based on VIN)
    if seen_vins is not None:
        rows = [r for r in rows if r.get("vin") not in seen_vins]
        # Update the seen_vins set
        for r in rows:
            if r.get("vin"):
                seen_vins.add(r.get("vin"))
    
    if not rows:
        return 0

    df = pd.DataFrame(rows)
    
    # Check if file exists to determine if we need to write the header
    file_exists = os.path.exists(csv_path)
    
    # Append to CSV (mode='a') instead of reading/rewriting the whole file
    try:
        df.to_csv(csv_path, mode='a', header=not file_exists, index=False)
        print(f"Appended {len(df)} new rows to CAR_DATA CSV")
        return len(df)
    except Exception as e:
        print(f"Error appending to CSV: {e}")
        return 0

# ====================================================================
# HTML scraping removed - using API data only
# ====================================================================


def car_data():
    max_retries = 5
    iteration = 1
    unclickable_buttons = set()
    seen_api_request_ids = set()
    
    # Initialize seen_vins to track duplicates in memory rather than reading CSV every time
    seen_vins = set()
    csv_path = os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv")
    if os.path.exists(csv_path):
        try:
            print("Loading existing VINs to avoid duplicates...")
            # Only read the VIN column to save memory/time
            existing_data = pd.read_csv(csv_path, usecols=["vin"])
            seen_vins = set(existing_data["vin"].dropna().astype(str).tolist())
            print(f"Loaded {len(seen_vins)} existing VINs")
        except Exception as e:
            print(f"Error loading existing VINs: {e}")
    
    # Enable CDP commands for API response capture
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
                    print(f"Skipping {button_name} button (already confirmed not clickable)")
                    continue
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        print(f"Attempting to click {button_name} button...")
                        
                        if handle_ribbon():
                            print("Ribbon found and dismissed")
                        
                        if click_button(button_xpath):
                            print(f"Successfully clicked {button_name} button")
                            any_button_clicked = True
                            break
                        else:
                            print(f"Button {button_name} not found or not clickable")
                            unclickable_buttons.add(button_name)
                            break
                        
                    except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                        print(f"\nConnection error during button click: {e}")
                        driver.quit()
                        sys.exit(0)
                    except Exception as e:
                        retry_count += 1
                        print(f"Error clicking {button_name} button (attempt {retry_count}): {str(e)}")
                        if retry_count < max_retries:
                            print("Retrying...")
                        else:
                            print(f"Failed to click {button_name} button after {max_retries} attempts")
            
            if not any_button_clicked:
                print("\nNo more buttons found to click. Finishing...")
                break
            
            # Collect API data after each iteration
            try:
                api_rows = parse_performance_logs_for_queue_results(seen_api_request_ids)
                if api_rows:
                    append_api_rows_to_csv(api_rows, seen_vins)
                    print(f"Captured {len(api_rows)} rows from iteration {iteration}")
            except Exception as e:
                print(f"Warning: Error collecting API data: {e}")
            
            loop_duration = time.time() - loop_start_time
            duration_msg = f"Iteration {iteration} took {loop_duration:.2f} seconds"
            print(duration_msg)
            logging.info(duration_msg)
            
            iteration += 1
            print(f"Completed iteration {iteration-1}, continuing to next iteration...")

    except Exception as e:
        print(f"\nUnexpected error: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        print(f"\nData collection completed. Output saved to CAR_DATA_{date.today()}.csv")

if __name__ == "__main__":
    car_data()
