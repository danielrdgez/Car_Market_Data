"""
Optimized Car Data Acquisition Module
======================================
High-performance web scraper for AutoTempest using Selenium with stealth mode
and API interception via Chrome DevTools Protocol.

Critical Performance Fixes:
- Performance log cleanup (prevents 3x slowdown after 100+ iterations)
- Batch database operations (10x faster inserts)
"""

import logging
import time
import random
import sys
import os
import json
from typing import Dict, Set, Optional, List
from datetime import date

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from selenium.webdriver.common.action_chains import ActionChains
from selenium_stealth import stealth
import urllib3

from database import CarDatabase


# ============================================================================
# CONFIGURATION
# ============================================================================

class Config:
    """Centralized configuration for the scraper"""
    # Search parameters
    INPUT_ZIP = "33186"
    INPUT_RADIUS = "50"
    INPUT_YEAR = 2000
    INPUT_STATE = "country"

    # Performance tuning
    DRIVER_RESTART_INTERVAL = 0   # Disabled to avoid state loss; set >0 to enable
    LOG_CLEANUP_INTERVAL = 1      # Clear performance logs every N iterations
    MAX_RETRIES = 3               # Retry failed operations

    # Timing (randomized for stealth)
    MIN_WAIT_AFTER_CLICK = 0.6
    MAX_WAIT_AFTER_CLICK = 1.2
    MIN_WAIT_FOR_SCROLL = 0.8
    MAX_WAIT_FOR_SCROLL = 1.5
    MIN_WAIT_BETWEEN_ITERATIONS = 2.1
    MAX_WAIT_BETWEEN_ITERATIONS = 4.5

    # Browser settings
    HEADLESS = True
    PAGE_LOAD_TIMEOUT = 60

    # Button XPaths
    CONTINUE_BUTTONS_XPATH = {
        "autotempest": '//*[@id="te-results"]/section/button',
        "hemmings": '//*[@id="hem-results"]/section/button',
        "cars": '//*[@id="cm-results"]/section/button',
        "carsoup": '//*[@id="cs-results"]/section/button',
        "carvana": '//*[@id="cv-results"]/section/button',
        "carmax": '//*[@id="cx-results"]/section/button',
        "autotrader": '//*[@id="at-results"]/section/button',
        "ebay": '//*[@id="eb-results"]/section/button',
        "other": '//*[@id="ot-results"]/section/button'
    }

    @classmethod
    def get_base_url(cls) -> str:
        """Generate the AutoTempest search URL"""
        return f'https://www.autotempest.com/results?localization={cls.INPUT_STATE}&zip={cls.INPUT_ZIP}'


class PerformanceMonitor:
    """Monitors and logs performance metrics"""

    def __init__(self):
        self.iteration_times: List[float] = []
        self.start_time = time.time()

    def log_iteration(self, iteration: int, duration: float, rows_processed: int):
        """Log iteration performance and detect degradation"""
        self.iteration_times.append(duration)
        avg_time = sum(self.iteration_times[-10:]) / min(len(self.iteration_times), 10)

        logging.info(
            f"Iteration {iteration}: {duration:.2f}s | "
            f"Rows: {rows_processed} | "
            f"Avg (last 10): {avg_time:.2f}s"
        )

        # Warning if performance degrades
        if duration > 45 and len(self.iteration_times) > 10:
            logging.warning(
                f"⚠️ Performance degradation detected! Current: {duration:.2f}s, "
                f"Avg: {avg_time:.2f}s"
            )
            print(f"⚠️ WARNING: Iteration took {duration:.2f}s (avg: {avg_time:.2f}s)")

    def get_total_runtime(self) -> float:
        """Get total runtime in seconds"""
        return time.time() - self.start_time

    def print_summary(self):
        """Print performance summary"""
        if not self.iteration_times:
            return

        total_time = self.get_total_runtime()
        avg_time = sum(self.iteration_times) / len(self.iteration_times)
        min_time = min(self.iteration_times)
        max_time = max(self.iteration_times)

        print("\n" + "="*60)
        print("PERFORMANCE SUMMARY")
        print("="*60)
        print(f"Total iterations: {len(self.iteration_times)}")
        print(f"Total runtime: {total_time/60:.2f} minutes")
        print(f"Average iteration time: {avg_time:.2f}s")
        print(f"Fastest iteration: {min_time:.2f}s")
        print(f"Slowest iteration: {max_time:.2f}s")
        print("="*60 + "\n")


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class CarScraper:
    """Main scraper class for AutoTempest data acquisition"""

    def __init__(self, config: Config = None):
        """Initialize the scraper with configuration"""
        self.config = config or Config()
        self.driver: Optional[webdriver.Chrome] = None
        self.db: Optional[CarDatabase] = None
        self.seen_vins: Set[str] = set()
        self.seen_api_request_ids: Set[str] = set()
        self.unclickable_buttons: Set[str] = set()
        self.performance_monitor = PerformanceMonitor()

        # Setup output directory
        self.output_directory = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "CAR_DATA_OUTPUT"
        )
        os.makedirs(self.output_directory, exist_ok=True)

        # Setup logging
        self._setup_logging()

        # Initialize database
        db_path = os.path.join(self.output_directory, "CAR_DATA.db")
        self.db = CarDatabase(db_path)

        logging.info("CarScraper initialized successfully")

    def _setup_logging(self):
        """Configure logging for the scraper"""
        log_path = os.path.join(self.output_directory, f'scraping_{date.today()}.log')

        # Clear any existing handlers
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )

        # Also log to console
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)

    def setup_driver(self) -> webdriver.Chrome:
        """Initialize a stealth-configured Chrome driver"""
        options = webdriver.ChromeOptions()

        # CRITICAL: Disable internal automation flags
        options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--no-sandbox')

        # Page load strategy for faster loading
        options.page_load_strategy = 'eager'

        # Enable Performance Logging for API Interception
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"})

        # Headless mode configuration
        if self.config.HEADLESS:
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

        # Set timeouts
        driver.set_page_load_timeout(self.config.PAGE_LOAD_TIMEOUT)
        driver.implicitly_wait(0)  # Use explicit waits

        logging.info("Chrome driver initialized with stealth configuration")
        return driver

    def restart_driver(self):
        """Restart the browser driver to prevent memory leaks"""
        logging.info("Restarting driver to clear memory and reset state...")

        # Save current URL
        current_url = self.driver.current_url if self.driver else self.config.get_base_url()

        # Cleanup old driver
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logging.warning(f"Error during driver quit: {e}")

        # Small delay to ensure cleanup
        time.sleep(2)

        # Reinitialize driver
        self.driver = self.setup_driver()

        # Enable CDP Network for API interception
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception as e:
            logging.warning(f"Could not enable Network CDP command: {e}")

        # Navigate back to current page
        self.driver.get(current_url)
        self._wait_for_page_load()

        # Clear the unclickable buttons set as page state has reset
        self.unclickable_buttons.clear()

        logging.info("✓ Driver restarted successfully")

    def _clear_performance_logs(self):
        """
        CRITICAL: Clear performance logs to prevent memory buildup
        This prevents the 3x slowdown after 100+ iterations
        """
        try:
            # Reading and discarding logs clears the buffer
            _ = self.driver.get_log("performance")
            logging.debug("Performance logs cleared")
        except Exception as e:
            logging.warning(f"Failed to clear performance logs: {e}")

    def _wait_for_page_load(self, timeout: int = 10) -> bool:
        """Wait for page to complete loading"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return True
        except TimeoutException:
            logging.warning('Timeout waiting for page load')
            return False

    def _click_button(self, xpath: str, timeout: int = 30) -> bool:
        """Click a button with robust fallbacks and randomized waits"""
        try:
            button = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )

            # Scroll button into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(random.uniform(self.config.MIN_WAIT_AFTER_CLICK, self.config.MAX_WAIT_AFTER_CLICK))

            # Try multiple click methods
            try:
                button.click()
            except:
                try:
                    ActionChains(self.driver).move_to_element(button).click().perform()
                except:
                    self.driver.execute_script("arguments[0].click();", button)

            # Wait for new content
            if self._wait_for_page_load(timeout):
                logging.info("New content loaded successfully")
                return True
            else:
                logging.info("No new content detected after click")
                return False

        except Exception as e:
            logging.debug(f"Failed to click button: {e}")
            return False

    def _handle_ribbon(self) -> bool:
        """Handle the potential appearance of a dismissible ribbon"""
        try:
            WebDriverWait(self.driver, 1).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
            )

            try:
                dismiss_button = WebDriverWait(self.driver, 1).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="cta-dismiss"]/i'))
                )
                self.driver.execute_script("arguments[0].scrollIntoView(true);", dismiss_button)
                time.sleep(random.uniform(self.config.MIN_WAIT_FOR_SCROLL, self.config.MAX_WAIT_FOR_SCROLL))
                ActionChains(self.driver).move_to_element(dismiss_button).click().perform()

                WebDriverWait(self.driver, 2).until(
                    EC.invisibility_of_element_located((By.XPATH, '//*[@id="results"]/div[9]/div'))
                )
                return True
            except Exception as e:
                logging.debug(f"Failed to dismiss ribbon: {e}")
                return False

        except TimeoutException:
            return False

    def _parse_performance_logs(self) -> List[Dict]:
        """Extract car data from 'queue-results' API responses in performance logs"""
        rows = []
        try:
            logs = self.driver.get_log("performance")

            for entry in logs:
                try:
                    msg = json.loads(entry["message"])['message']
                except Exception:
                    continue

                # Only process Network responses
                if msg.get("method") != "Network.responseReceived":
                    continue

                params = msg.get("params", {})
                response = params.get("response", {})
                url = response.get("url", "")

                # Only process queue-results endpoints
                if "queue-results" not in url:
                    continue

                request_id = params.get("requestId")

                # Skip if already processed
                if not request_id or request_id in self.seen_api_request_ids:
                    continue

                self.seen_api_request_ids.add(request_id)

                # Get response body via CDP
                try:
                    body = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    text = body.get("body", "")

                    if not text:
                        continue

                    api_json = json.loads(text)
                    rows.extend(self._extract_rows_from_api_data(api_json))
                    logging.info(f"Captured queue-results response (requestId={request_id})")

                except WebDriverException:
                    continue

        except Exception as e:
            logging.error(f"Error reading performance logs: {e}")

        return rows

    def _extract_rows_from_api_data(self, api_data: Dict) -> List[Dict]:
        """Extract and format car data from the API JSON response"""
        rows = []
        items = api_data.get("items") or api_data.get("results") or []

        fields_to_extract = [
            "date", "location", "locationCode", "countryCode",
            "pendingSale", "title", "currentBid", "bids", "distance", "priceHistory",
            "priceRecentChange", "price", "listingHistory", "mileage", "year", "vin",
            "sellerType", "vehicleTitle", "listingType", "vehicleTitleDesc",
            "sourceName", "img"
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

    def run(self):
        """Main execution loop for data collection"""
        logging.info("="*60)
        logging.info("Starting Car Data Acquisition")
        logging.info("="*60)

        # Initialize driver
        self.driver = self.setup_driver()

        # Enable CDP Network
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception as e:
            logging.warning(f"Could not enable Network CDP command: {e}")

        # Load existing VINs
        logging.info("Loading existing VINs from database...")
        self.seen_vins = self.db.get_seen_vins()
        logging.info(f"Loaded {len(self.seen_vins)} existing VINs")

        # Navigate to search page
        logging.info(f"Navigating to: {self.config.get_base_url()}")
        self.driver.get(self.config.get_base_url())
        self._wait_for_page_load()

        iteration = 1

        try:
            while True:
                loop_start_time = time.time()
                logging.info(f"\n{'='*60}")
                logging.info(f"Starting iteration {iteration}")
                logging.info(f"{'='*60}")

                # Periodic driver restart to prevent memory leaks
                if (
                    self.config.DRIVER_RESTART_INTERVAL
                    and iteration > 1
                    and iteration % self.config.DRIVER_RESTART_INTERVAL == 0
                ):
                    self.restart_driver()

                # Try to click load more buttons
                any_button_clicked = False

                for button_name, button_xpath in self.config.CONTINUE_BUTTONS_XPATH.items():
                    # Skip buttons we know are unclickable
                    if button_name in self.unclickable_buttons:
                        continue

                    retry_count = 0
                    while retry_count < self.config.MAX_RETRIES:
                        try:
                            logging.info(f"Attempting to click '{button_name}' button...")

                            # Handle ribbon if present
                            if self._handle_ribbon():
                                logging.info("Ribbon found and dismissed")

                            # Try to click button
                            if self._click_button(button_xpath):
                                logging.info(f"Successfully clicked '{button_name}' button")
                                print(f"✓ Clicked '{button_name}' button")
                                any_button_clicked = True
                                break
                            else:
                                logging.info(f"Button '{button_name}' not found or not clickable")
                                self.unclickable_buttons.add(button_name)
                                break

                        except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                            logging.error(f"Connection error during button click: {e}")
                            self.cleanup()
                            sys.exit(1)

                        except Exception as e:
                            retry_count += 1
                            logging.warning(f"Error clicking '{button_name}' (attempt {retry_count}): {e}")

                            if retry_count >= self.config.MAX_RETRIES:
                                logging.error(f"Failed to click '{button_name}' after {self.config.MAX_RETRIES} attempts")

                # Parse API data from performance logs
                api_rows = []
                try:
                    api_rows = self._parse_performance_logs()

                    if api_rows:
                        inserted = self.db.insert_rows(api_rows)
                        for r in api_rows:
                            if r.get("vin"):
                                self.seen_vins.add(r.get("vin"))
                        logging.info(f"Processed {len(api_rows)} rows, inserted/updated {inserted} records")
                        print(f"✓ Iteration {iteration}: Processed {len(api_rows)} rows, inserted {inserted} records")
                    else:
                        logging.info("No new data found in this iteration")

                except Exception as e:
                    logging.error(f"Error collecting API data: {e}")

                # CRITICAL: Clear performance logs to prevent memory buildup
                if iteration % self.config.LOG_CLEANUP_INTERVAL == 0:
                    self._clear_performance_logs()

                # Check if we should continue
                if not any_button_clicked:
                    logging.info("No more active 'load more' buttons found. Finishing...")
                    print("\n✓ Scraping complete - no more data to load")
                    break

                # Log performance metrics
                loop_duration = time.time() - loop_start_time
                self.performance_monitor.log_iteration(iteration, loop_duration, len(api_rows))

                # Random delay between iterations (stealth)
                delay = random.uniform(
                    self.config.MIN_WAIT_BETWEEN_ITERATIONS,
                    self.config.MAX_WAIT_BETWEEN_ITERATIONS
                )
                logging.info(f"Waiting {delay:.2f}s before next iteration...")
                time.sleep(delay)

                iteration += 1

        except KeyboardInterrupt:
            logging.info("\nScraping interrupted by user")
            print("\n⚠ Scraping interrupted by user")

        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
            print(f"\n✗ Error: {e}")

        finally:
            self.cleanup()

    def cleanup(self):
        """Clean up resources"""
        logging.info("Cleaning up resources...")

        # Print performance summary
        self.performance_monitor.print_summary()

        # Close database connection
        if self.db:
            self.db.close()

        # Quit driver
        if self.driver:
            try:
                self.driver.quit()
                logging.info("Browser driver closed")
            except Exception as e:
                logging.warning(f"Error closing driver: {e}")

        logging.info("Cleanup complete")
        print(f"\nData collection finished. Output saved to {os.path.join(self.output_directory, 'CAR_DATA.db')}")


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Entry point for the scraper"""
    scraper = CarScraper()
    scraper.run()


if __name__ == "__main__":
    main()

