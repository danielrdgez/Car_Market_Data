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
from datetime import date
import os
import urllib3
import sys

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
    
def car_df(existing_df=None):
    html = driver.page_source
    soup = BeautifulSoup(html, "html.parser")
    section_card = soup.find_all("div", class_="description-wrap")
    
    car_dictionary = {
        "price" : [],
        "mileage" : [],
        "year" : [],
        "make" : [],
        "model" : [],
        "trim" : [],
        "distance_from_zip" : [],
        "city" : [],
        "time_listed" : [],
        "branded_title" : [],
        "current_bid" : [],
        "description" : []
    }

    for section in section_card:
        price = section.find("div", class_= "badge__label label--price")

        # Track if price is a current bid
        is_bid = False

        if price is None:
            bid_price = section.find("div", class_="badge__label label--bid")
            history_price = section.find("div" , class_= "badge__label label--price price-history")

            if bid_price is not None:
                price = bid_price
                is_bid = True
            elif history_price is not None:
                price = history_price

        if price != None:
            price_car = price.text
            car_dictionary["price"].append(price_car)
        else:
            car_dictionary["price"].append("Inquire")

        car_dictionary["current_bid"].append(1 if is_bid else 0)

        mileage = section.find("span", class_="mileage")
        if mileage != None:
            mileage_car = mileage.text
            car_dictionary["mileage"].append(mileage_car)
        else:
            car_dictionary["mileage"].append(f"None")
                        
        name = section.find("span", class_="title-wrap listing-title")
        if name != None:
            name_car = name.text.strip().split(" ")
            # Check if the list has enough elements before accessing them
            car_dictionary["year"].append(name_car[0] if len(name_car) > 0 else "")
            car_dictionary["make"].append(name_car[1] if len(name_car) > 1 else "")
            car_dictionary["model"].append(name_car[2] if len(name_car) > 2 else "")
            # Join trim words into a single string
            trim_words = name_car[3:] if len(name_car) > 3 else []
            car_dictionary["trim"].append(" ".join(trim_words) if trim_words else "")
            
        distance = section.find("span", class_="distance")
        if distance != None:
            distance_car = distance.text
            car_dictionary["distance_from_zip"].append(distance_car)
        else:
            car_dictionary["distance_from_zip"].append(f"delivers to {input_zip}")

        city = section.find("span", class_="city")
        if city != None:
            city_car = city.text
            car_dictionary["city"].append(city_car)
        else:
            car_dictionary["city"].append("")

        time_listed = section.find("span", class_="date")
        if time_listed != None:
            time_listed_car = time_listed.text
            car_dictionary["time_listed"].append(time_listed_car)
        else:
            car_dictionary["time_listed"].append("")

        branded_title = section.find("span", class_="title-status")
        if branded_title != None:
            branded_title_car = 1
            car_dictionary["branded_title"].append(branded_title_car)
        else:
            car_dictionary["branded_title"].append(0)
        
        # Extract description from the details div containing 4 spans
        details_div = section.find("div", class_="details")
        if details_div:
            spans = details_div.find_all("span")
            description_text = " ".join([span.text.strip() for span in spans if span.text])
            car_dictionary["description"].append(description_text if description_text else "")
        else:
            car_dictionary["description"].append("")
    
    new_df = pd.DataFrame.from_dict(car_dictionary)
    
    # Add load date to the new dataframe
    new_df["load_date"] = date.today()
    
    if existing_df is not None:
        # Concatenate new data with existing data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        # Drop duplicates based on all columns except 'distance from zip' and 'load_date'
        columns_for_dupes = [col for col in combined_df.columns if col not in ['distance from zip', 'load_date']]
        combined_df = combined_df.drop_duplicates(subset=columns_for_dupes, keep='first')
        
        # Save the current state to CSV
        combined_df.to_csv(os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv"), index=False)
        print(f"Saved {len(combined_df)} cars to CSV file")
        
        return combined_df
    else:
        # Save the initial state to CSV
        new_df.to_csv(os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv"), index=False)
        print(f"Saved {len(new_df)} cars to CSV file")
        
        return new_df

def save_and_exit(accumulated_df=None, driver=None):
    """Helper function to save data and clean up before exiting"""
    if accumulated_df is not None:
        try:
            # Final save of the data
            accumulated_df["load_date"] = date.today()
            accumulated_df.to_csv(os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv"), index=False)
            print(f"\nFinal save completed. Total cars collected: {len(accumulated_df)}")
        except Exception as e:
            print(f"Error saving final data: {e}")
    
    if driver is not None:
        try:
            driver.quit()
        except Exception:
            pass
    
    sys.exit(0)

def car_data():
    max_retries = 5  # Increased maximum retries
    iteration = 1
    accumulated_df = None
    ribbon_handled = False
    unclickable_buttons = set()  # Track buttons that are confirmed not clickable
    
    def reset_driver():
        """Helper function to reset the driver if it becomes unresponsive"""
        global driver
        try:
            driver.quit()
        except:
            pass
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(120)
        driver.implicitly_wait(5)
        driver.get(f'https://www.autotempest.com/results?localization={input_state}&zip={input_zip}&minprice=1')
        time.sleep(5)  # Wait for initial page load
    
    try:
        while True:  # Continue until no buttons are clickable
            print(f"\nStarting iteration {iteration}")
            
            try:
                # Get current car count before clicking buttons
                accumulated_df = car_df(accumulated_df)  # Pass the existing dataframe
                print(f"Current number of cars before iteration {iteration}: {len(accumulated_df)}")
            except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                print(f"\nConnection error while getting car data: {e}")
                save_and_exit(accumulated_df, driver)
            
            any_button_clicked = False  # Track if any button was successfully clicked
            
            for button_name, button_xpath in continue_buttons_xpath.items():
                # Skip buttons that are already confirmed as unclickable
                if button_name in unclickable_buttons:
                    print(f"Skipping {button_name} button (already confirmed not clickable)")
                    continue
                
                retry_count = 0
                while retry_count < max_retries:
                    try:
                        print(f"Attempting to click {button_name} button...")
                        
                        # Check for ribbon before clicking each button
                        if handle_ribbon():
                            print("Ribbon found and dismissed")
                        
                        # Try to click the button
                        if click_button(button_xpath):
                            print(f"Successfully clicked {button_name} button")
                            any_button_clicked = True  # Mark that we clicked at least one button
                            break  # Success - move to next button
                        else:
                            print(f"Button {button_name} not found or not clickable")
                            unclickable_buttons.add(button_name)  # Mark as unclickable for future iterations
                            break  # Button not found - move to next button
                        
                    except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                        print(f"\nConnection error during button click: {e}")
                        save_and_exit(accumulated_df, driver)
                    except Exception as e:
                        retry_count += 1
                        print(f"Error clicking {button_name} button (attempt {retry_count}): {str(e)}")
                        if retry_count < max_retries:
                            print("Retrying...")
                        else:
                            print(f"Failed to click {button_name} button after {max_retries} attempts")
            
            # If no buttons were clicked in this iteration, we're done
            if not any_button_clicked:
                print("\nNo more buttons found to click. Finishing...")
                break
                
            try:
                # Get car count after clicking all buttons in this iteration
                accumulated_df = car_df(accumulated_df)  # Pass the accumulated dataframe
                print(f"Number of cars after iteration {iteration}: {len(accumulated_df)}")
                
                # Save progress
                accumulated_df["load_date"] = date.today()
                accumulated_df.to_csv(os.path.join(output_directory, f"CAR_DATA_{date.today()}.csv"), index=False)
                print(f"Saved current progress to CSV file")
            except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                print(f"\nConnection error while saving progress: {e}")
                save_and_exit(accumulated_df, driver)
            
            iteration += 1
            print(f"Completed iteration {iteration-1}, continuing to next iteration...")

    except Exception as e:
        print(f"\nUnexpected error: {e}")
        save_and_exit(accumulated_df, driver)
    
    # Normal completion
    save_and_exit(accumulated_df, driver)

if __name__ == "__main__":
    car_data()
