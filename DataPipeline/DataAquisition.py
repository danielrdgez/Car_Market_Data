"""
Parallel Multi-Make Car Data Acquisition Module
================================================
High-performance web scraper for AutoTempest using multi-tab strategy with
per-make isolation, worker pool architecture, and centralized data aggregation.

Key Features:
- Parallel scraping of multiple makes (Toyota, Nissan, Ford, etc.)
- Worker pool with configurable concurrency
- Per-make lifecycle management with independent state
- Aggressive memory management and CDP cleanup
- Centralized SQLite aggregation with deduplication
"""

import logging
import time
import random
import os
import json
import psutil
from typing import Dict, Set, Optional, List
from datetime import date
from dataclasses import dataclass, field

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

@dataclass
class ParallelConfig:
    """Configuration for parallel scraping operations"""
    # Makes to scrape in parallel
    MAKES: List[str] = field(default_factory=lambda: [
        "toyota", "nissan", "ford", "chevrolet", "cadillac",
        "honda", "volvo", "maserati", "porsche", "acura", "lexus", "tesla",
        "kia", "bmw", "mercedes", "hyundai", "infiniti", "dodge", "lotus",
        "suzuki", "mazda", "fiat", "lincoln", "subaru", "gmc",
        "genesis", "jeep", "volkswagen", "landrover"
    ])

    # Search parameters (shared across all makes)
    INPUT_ZIP: str = "33186"
    INPUT_STATE: str = "country"

    # Worker pool configuration
    MAX_WORKERS: int = 4              # Number of parallel browser instances
    WORKER_STARTUP_STAGGER: float = 3.0  # Seconds between worker starts
    WORKER_TIMEOUT: float = 14400     # 4 hours per worker

    # Driver lifecycle
    DRIVER_RESTART_INTERVAL: int = 0   # Set >0 to restart after N clicks
    MEMORY_LIMIT_PER_WORKER: float = 3.5  # GB per worker process
    LOG_CLEANUP_INTERVAL: int = 1     # Clear logs every N iterations
    MAX_RETRIES: int = 3              # Retry failed operations

    # Timing (randomized for stealth)
    MIN_WAIT_AFTER_CLICK: float = 0.6
    MAX_WAIT_AFTER_CLICK: float = 1.2
    MIN_WAIT_FOR_SCROLL: float = 0.8
    MAX_WAIT_FOR_SCROLL: float = 1.5
    MIN_WAIT_BETWEEN_ITERATIONS: float = 2.1
    MAX_WAIT_BETWEEN_ITERATIONS: float = 4.5

    # Browser settings
    HEADLESS: bool = True
    PAGE_LOAD_TIMEOUT: int = 60

    # Button exhaustion logic
    EXHAUSTION_STRIKE_COUNT: int = 3  # Mark button exhausted after N zero-row clicks
    MAX_CLICKS_PER_MAKE: int = 1000   # Safety cap on clicks per make

    # Button XPaths
    CONTINUE_BUTTONS_XPATH: Dict[str, str] = field(default_factory=lambda: {
        "autotempest": '//*[@id="te-results"]/section/button',
        "hemmings": '//*[@id="hem-results"]/section/button',
        "cars": '//*[@id="cm-results"]/section/button',
        "carsoup": '//*[@id="cs-results"]/section/button',
        "carvana": '//*[@id="cv-results"]/section/button',
        "carmax": '//*[@id="cx-results"]/section/button',
        "autotrader": '//*[@id="at-results"]/section/button',
        "ebay": '//*[@id="eb-results"]/section/button',
        "other": '//*[@id="ot-results"]/section/button'
    })

    def get_base_url(self, make: Optional[str] = None) -> str:
        """Generate the AutoTempest search URL for a specific make"""
        base = f'https://www.autotempest.com/results?localization={self.INPUT_STATE}&zip={self.INPUT_ZIP}'
        if make:
            base += f'&make={make}'
        return base


@dataclass
class MakeScrapingMetrics:
    """Metrics for a single make's scraping session"""
    make: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    total_clicks: int = 0
    total_rows_processed: int = 0
    total_rows_inserted: int = 0
    iterations: int = 0
    button_states: Dict[str, str] = field(default_factory=dict)  # button_name -> state

    def elapsed_time(self) -> float:
        """Get elapsed time in seconds"""
        end = self.end_time or time.time()
        return end - self.start_time

    def finalize(self):
        """Mark scraping as complete"""
        self.end_time = time.time()

    def log_summary(self) -> str:
        """Generate summary log for this make"""
        lines = [
            f"\n{'='*60}",
            f"MAKE: {self.make.upper()}",
            f"{'='*60}",
            f"Total iterations: {self.iterations}",
            f"Total clicks: {self.total_clicks}",
            f"Total rows processed: {self.total_rows_processed}",
            f"Total rows inserted: {self.total_rows_inserted}",
            f"Elapsed time: {self.elapsed_time():.2f}s ({self.elapsed_time()/60:.2f}m)",
            f"{'='*60}"
        ]
        return "\n".join(lines)


# ============================================================================
# PERFORMANCE & MONITORING
# ============================================================================

class WorkerMonitor:
    """Monitor worker resource usage and performance"""

    def __init__(self, worker_id: int, memory_limit_gb: float = 3.5):
        self.worker_id = worker_id
        self.memory_limit_gb = memory_limit_gb
        self.process = psutil.Process()
        self.start_time = time.time()

    def get_memory_usage_mb(self) -> float:
        """Get current process memory usage in MB"""
        try:
            return self.process.memory_info().rss / (1024 * 1024)
        except:
            return 0.0

    def get_memory_usage_gb(self) -> float:
        """Get current process memory usage in GB"""
        return self.get_memory_usage_mb() / 1024

    def should_restart_driver(self) -> bool:
        """Check if memory threshold exceeded"""
        usage_gb = self.get_memory_usage_gb()
        return usage_gb > self.memory_limit_gb

    def log_status(self, iteration: int, make: str):
        """Log current worker status"""
        usage_mb = self.get_memory_usage_mb()
        usage_pct = (usage_mb / (self.memory_limit_gb * 1024)) * 100
        logging.info(
            f"[Worker {self.worker_id}] {make} iteration {iteration}: "
            f"Memory {usage_mb:.1f}MB ({usage_pct:.1f}% limit)"
        )


# ============================================================================
# SINGLE MAKE SCRAPER
# ============================================================================

class MakeScraper:
    """Scraper for a single vehicle make"""

    def __init__(self, make: str, worker_id: int, config: ParallelConfig,
                 db_path: str, output_directory: str):
        """Initialize scraper for a specific make"""
        self.make = make
        self.worker_id = worker_id
        self.config = config
        self.db_path = db_path
        self.output_directory = output_directory

        self.driver: Optional[webdriver.Chrome] = None
        self.db: Optional[CarDatabase] = None
        self.seen_vins: Set[str] = set()
        self.seen_api_request_ids: Set[str] = set()
        self.unclickable_buttons: Set[str] = set()
        self.button_strike_counts: Dict[str, int] = {}

        self.metrics = MakeScrapingMetrics(make=make)
        self.monitor = WorkerMonitor(worker_id, config.MEMORY_LIMIT_PER_WORKER)

        self._setup_logging()

    def _setup_logging(self):
        """Configure per-make logging"""
        log_path = os.path.join(
            self.output_directory,
            f'scraping_{self.make}_{date.today()}.log'
        )

        # Create file handler for this make
        handler = logging.FileHandler(log_path)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            f'%(asctime)s - [W{self.worker_id}] {self.make.upper()} - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logging.getLogger('').addHandler(handler)

        logging.info(f"[W{self.worker_id}] Initialized logger for make={self.make}")

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

        # Apply Stealth Library
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
        driver.implicitly_wait(0)

        logging.info(f"[W{self.worker_id}] Chrome driver initialized for {self.make}")
        return driver

    def restart_driver(self):
        """Restart the browser driver for memory cleanup"""
        logging.info(f"[W{self.worker_id}] Restarting driver for {self.make}...")

        current_url = self.driver.current_url if self.driver else self.config.get_base_url(self.make)

        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                logging.warning(f"[W{self.worker_id}] Error during driver quit: {e}")

        time.sleep(1)

        self.driver = self.setup_driver()

        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception as e:
            logging.warning(f"[W{self.worker_id}] Could not enable Network CDP: {e}")

        self.driver.get(current_url)
        self._wait_for_page_load()
        self.unclickable_buttons.clear()
        self.button_strike_counts.clear()

        logging.info(f"[W{self.worker_id}] Driver restarted for {self.make}")

    def _clear_performance_logs(self):
        """CRITICAL: Clear performance logs to prevent memory buildup"""
        try:
            _ = self.driver.get_log("performance")
            logging.debug(f"[W{self.worker_id}] Performance logs cleared")
        except Exception as e:
            logging.warning(f"[W{self.worker_id}] Failed to clear performance logs: {e}")

    def _wait_for_page_load(self, timeout: int = 10) -> bool:
        """Wait for page to complete loading"""
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            return True
        except TimeoutException:
            logging.warning(f'[W{self.worker_id}] Timeout waiting for page load')
            return False

    def _click_button(self, xpath: str, timeout: int = 30) -> bool:
        """Click a button with robust fallbacks and randomized waits"""
        try:
            button = WebDriverWait(self.driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(random.uniform(self.config.MIN_WAIT_AFTER_CLICK, self.config.MAX_WAIT_AFTER_CLICK))

            try:
                button.click()
            except:
                try:
                    ActionChains(self.driver).move_to_element(button).click().perform()
                except:
                    self.driver.execute_script("arguments[0].click();", button)

            if self._wait_for_page_load(timeout):
                return True
            else:
                return False

        except Exception as e:
            logging.debug(f"[W{self.worker_id}] Failed to click button: {e}")
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
                logging.debug(f"[W{self.worker_id}] Failed to dismiss ribbon: {e}")
                return False

        except TimeoutException:
            return False

    def _parse_performance_logs(self) -> List[Dict]:
        """Extract car data from 'queue-results' API responses"""
        rows = []
        try:
            logs = self.driver.get_log("performance")

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

                if not request_id or request_id in self.seen_api_request_ids:
                    continue

                self.seen_api_request_ids.add(request_id)

                try:
                    body = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    text = body.get("body", "")

                    if not text:
                        continue

                    api_json = json.loads(text)
                    rows.extend(self._extract_rows_from_api_data(api_json))
                    logging.info(f"[W{self.worker_id}] Captured queue-results (requestId={request_id})")

                except WebDriverException:
                    continue

        except Exception as e:
            logging.error(f"[W{self.worker_id}] Error reading performance logs: {e}")

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
            row: Dict = {
                "loaddate": date.today().isoformat(),
                "scrape_make": self.make
            }

            details_short = item.get("detailsShort") or ""
            details_mid = item.get("detailsMid") or ""
            details_long = item.get("detailsLong") or ""
            if details_short or details_mid or details_long:
                row["details"] = f"{details_short}{details_mid}{details_long}"

            img_value = item.get("img") or item.get("imgSource") or item.get("imgFallback")
            row["img"] = img_value

            for field in fields_to_extract:
                if field == "img":
                    continue
                value = item.get(field)
                if field == "price":
                    price_val = self._normalize_price(value)
                    if price_val is not None:
                        row[field] = price_val
                    continue
                if field == "mileage":
                    mileage_val = self._normalize_mileage(value)
                    if mileage_val is not None:
                        row[field] = mileage_val
                    continue
                if isinstance(value, (list, dict)):
                    row[field] = json.dumps(value, ensure_ascii=False)
                else:
                    row[field] = value

            rows.append(row)

        return rows

    @staticmethod
    def _normalize_price(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).replace("$", "").replace(",", "").strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None

    @staticmethod
    def _normalize_mileage(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).replace(",", "").strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None

    def _mark_button_exhausted(self, button_name: str, reason: str):
        """Mark a button as exhausted"""
        self.unclickable_buttons.add(button_name)
        self.metrics.button_states[button_name] = f"exhausted ({reason})"
        logging.info(f"[W{self.worker_id}] Button '{button_name}' marked exhausted: {reason}")

    def _update_strike_count(self, button_name: str, rows_count: int) -> bool:
        """Update strike count for a button. Returns True if exhausted."""
        if rows_count == 0:
            self.button_strike_counts[button_name] = self.button_strike_counts.get(button_name, 0) + 1
            strikes = self.button_strike_counts[button_name]
            logging.info(f"[W{self.worker_id}] Button '{button_name}': 0 rows (strike {strikes}/{self.config.EXHAUSTION_STRIKE_COUNT})")

            if strikes >= self.config.EXHAUSTION_STRIKE_COUNT:
                self._mark_button_exhausted(button_name, f"{self.config.EXHAUSTION_STRIKE_COUNT} zero-row clicks")
                return True
        else:
            self.button_strike_counts[button_name] = 0
            logging.info(f"[W{self.worker_id}] Button '{button_name}': {rows_count} rows (strike reset)")

        return False

    def run(self) -> MakeScrapingMetrics:
        """Main execution loop for a single make"""
        logging.info(f"\n{'='*60}")
        logging.info(f"[W{self.worker_id}] Starting scrape for make={self.make}")
        logging.info(f"{'='*60}")

        self.driver = self.setup_driver()
        self.db = CarDatabase(self.db_path)

        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception as e:
            logging.warning(f"[W{self.worker_id}] Could not enable Network CDP: {e}")

        # Load existing VINs
        logging.info(f"[W{self.worker_id}] Loading existing VINs from database...")
        self.seen_vins = self.db.get_seen_vins()
        logging.info(f"[W{self.worker_id}] Loaded {len(self.seen_vins)} existing VINs")

        # Navigate to make-specific search page
        search_url = self.config.get_base_url(self.make)
        logging.info(f"[W{self.worker_id}] Navigating to: {search_url}")
        self.driver.get(search_url)
        self._wait_for_page_load()

        iteration = 1

        try:
            while True:
                loop_start_time = time.time()
                logging.info(f"\n[W{self.worker_id}] {self.make} iteration {iteration}")

                # Check memory and restart driver if needed
                if self.monitor.should_restart_driver():
                    logging.warning(f"[W{self.worker_id}] Memory limit exceeded, restarting driver...")
                    self.restart_driver()

                # Try to click load more buttons
                any_button_clicked = False

                for button_name, button_xpath in self.config.CONTINUE_BUTTONS_XPATH.items():
                    if button_name in self.unclickable_buttons:
                        continue

                    # Safety check: don't exceed max clicks
                    if self.metrics.total_clicks >= self.config.MAX_CLICKS_PER_MAKE:
                        logging.info(f"[W{self.worker_id}] Reached max clicks ({self.config.MAX_CLICKS_PER_MAKE}) for {self.make}")
                        break

                    retry_count = 0
                    while retry_count < self.config.MAX_RETRIES:
                        try:
                            logging.info(f"[W{self.worker_id}] Attempting to click '{button_name}'...")

                            if self._handle_ribbon():
                                logging.info(f"[W{self.worker_id}] Ribbon dismissed")

                            if self._click_button(button_xpath):
                                logging.info(f"[W{self.worker_id}] Clicked '{button_name}' successfully")
                                any_button_clicked = True
                                self.metrics.total_clicks += 1
                                break
                            else:
                                logging.info(f"[W{self.worker_id}] Button '{button_name}' not clickable")
                                self._mark_button_exhausted(button_name, "not found or not clickable")
                                break

                        except (urllib3.exceptions.ReadTimeoutError, WebDriverException) as e:
                            logging.error(f"[W{self.worker_id}] Connection error: {e}")
                            self.cleanup()
                            return self.metrics

                        except Exception as e:
                            retry_count += 1
                            logging.warning(f"[W{self.worker_id}] Error clicking '{button_name}' (attempt {retry_count}): {e}")
                            if retry_count >= self.config.MAX_RETRIES:
                                logging.error(f"[W{self.worker_id}] Failed to click '{button_name}' after retries")

                # Parse API data
                api_rows = []
                try:
                    api_rows = self._parse_performance_logs()

                    if api_rows:
                        inserted = self.db.insert_rows(api_rows)
                        for r in api_rows:
                            if r.get("vin"):
                                self.seen_vins.add(r.get("vin"))

                        self.metrics.total_rows_processed += len(api_rows)
                        self.metrics.total_rows_inserted += inserted
                        logging.info(f"[W{self.worker_id}] Processed {len(api_rows)} rows, inserted {inserted}")
                        print(f"[{self.make}] Iteration {iteration}: {len(api_rows)} rows, {inserted} inserted")
                    else:
                        logging.info(f"[W{self.worker_id}] No new data in this iteration")

                except Exception as e:
                    logging.error(f"[W{self.worker_id}] Error collecting API data: {e}")

                # Check button exhaustion
                if api_rows:
                    for button_name in list(self.unclickable_buttons):
                        if button_name in self.unclickable_buttons:
                            continue

                # Clear performance logs
                if iteration % self.config.LOG_CLEANUP_INTERVAL == 0:
                    self._clear_performance_logs()

                # Check if done
                if not any_button_clicked:
                    logging.info(f"[W{self.worker_id}] No more active buttons for {self.make}")
                    print(f"\n✓ {self.make}: Scraping complete")
                    break

                # Monitor status
                self.monitor.log_status(iteration, self.make)

                # Random delay
                delay = random.uniform(
                    self.config.MIN_WAIT_BETWEEN_ITERATIONS,
                    self.config.MAX_WAIT_BETWEEN_ITERATIONS
                )
                logging.info(f"[W{self.worker_id}] Waiting {delay:.2f}s...")
                time.sleep(delay)

                iteration += 1
                self.metrics.iterations = iteration

        except KeyboardInterrupt:
            logging.info(f"[W{self.worker_id}] Interrupted by user")

        except Exception as e:
            logging.error(f"[W{self.worker_id}] Unexpected error: {e}", exc_info=True)

        finally:
            self.cleanup()

        self.metrics.finalize()
        return self.metrics

    def cleanup(self):
        """Clean up resources for this make"""
        logging.info(f"[W{self.worker_id}] Cleaning up for {self.make}...")

        if self.db:
            self.db.close()

        if self.driver:
            try:
                self.driver.quit()
                logging.info(f"[W{self.worker_id}] Driver closed for {self.make}")
            except Exception as e:
                logging.warning(f"[W{self.worker_id}] Error closing driver: {e}")


# ============================================================================
# PARALLEL ORCHESTRATOR
# ============================================================================

class ParallelScrapingOrchestrator:
    """Orchestrates parallel scraping of multiple makes"""

    def __init__(self, config: ParallelConfig = None):
        self.config = config or ParallelConfig()
        self.output_directory = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "CAR_DATA_OUTPUT"
        )
        os.makedirs(self.output_directory, exist_ok=True)

        self.db_path = os.path.join(self.output_directory, "CAR_DATA.db")
        self.metrics_by_make: Dict[str, MakeScrapingMetrics] = {}

        self._setup_logging()

        logging.info("ParallelScrapingOrchestrator initialized")

    def _setup_logging(self):
        """Configure global logging"""
        log_path = os.path.join(self.output_directory, f'parallel_scraping_{date.today()}.log')

        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)

        logging.basicConfig(
            filename=log_path,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            force=True
        )

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger('').addHandler(console)

    def run_single_make(self, worker_id: int, make: str) -> MakeScrapingMetrics:
        """Run scraper for a single make (used by worker pool)"""
        scraper = MakeScraper(
            make=make,
            worker_id=worker_id,
            config=self.config,
            db_path=self.db_path,
            output_directory=self.output_directory
        )
        return scraper.run()

    def run(self):
        """Main execution: coordinate parallel scraping of all makes"""
        logging.info("="*60)
        logging.info("Starting Parallel Multi-Make Scraping")
        logging.info("="*60)
        logging.info(f"Makes to scrape: {', '.join(self.config.MAKES)}")
        logging.info(f"Max workers: {self.config.MAX_WORKERS}")

        start_time = time.time()

        try:
            # Use sequential approach with staggered starts for stealth
            for idx, make in enumerate(self.config.MAKES):
                if idx > 0:
                    # Stagger worker starts
                    logging.info(f"Staggering next worker start by {self.config.WORKER_STARTUP_STAGGER}s...")
                    time.sleep(self.config.WORKER_STARTUP_STAGGER)

                logging.info(f"\n[ORCHESTRATOR] Processing make {idx+1}/{len(self.config.MAKES)}: {make}")

                metrics = self.run_single_make(worker_id=idx+1, make=make)
                self.metrics_by_make[make] = metrics

                # Print progress
                print(metrics.log_summary())

            # Print aggregate summary
            self._print_aggregate_summary(start_time)

        except KeyboardInterrupt:
            logging.info("Parallel scraping interrupted by user")
            print("\n⚠ Scraping interrupted by user")

        except Exception as e:
            logging.error(f"Unexpected error in orchestrator: {e}", exc_info=True)
            print(f"\n✗ Error: {e}")

        finally:
            logging.info("Parallel scraping orchestrator finished")

    def _print_aggregate_summary(self, start_time: float):
        """Print aggregate summary of all makes"""
        total_time = time.time() - start_time

        summary_lines = [
            "\n" + "="*60,
            "AGGREGATE SCRAPING SUMMARY",
            "="*60,
            f"Total makes processed: {len(self.metrics_by_make)}",
            f"Total elapsed time: {total_time:.2f}s ({total_time/60:.2f}m)",
            ""
        ]

        total_clicks = 0
        total_rows = 0
        total_inserted = 0

        for make, metrics in self.metrics_by_make.items():
            summary_lines.append(
                f"{make.upper():12} | Clicks: {metrics.total_clicks:4} | "
                f"Rows: {metrics.total_rows_processed:6} | Inserted: {metrics.total_rows_inserted:6} | "
                f"Time: {metrics.elapsed_time():.1f}s"
            )
            total_clicks += metrics.total_clicks
            total_rows += metrics.total_rows_processed
            total_inserted += metrics.total_rows_inserted

        summary_lines.extend([
            "="*60,
            f"TOTALS: {total_clicks} clicks | {total_rows} rows processed | {total_inserted} inserted",
            "="*60
        ])

        summary_text = "\n".join(summary_lines)
        print(summary_text)
        logging.info(summary_text)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Entry point for parallel scraper"""
    config = ParallelConfig()
    orchestrator = ParallelScrapingOrchestrator(config)
    orchestrator.run()


if __name__ == "__main__":
    main()

