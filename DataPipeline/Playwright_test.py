"""
Per-Button Parallel Car Data Acquisition Module (Playwright Edition)
=====================================================================
Each source button (autotempest, hemmings, cars, etc.) gets its own dedicated
browser instance running in a separate thread. Buttons are clicked until true
exhaustion (3 consecutive zero-row responses). A shared thread-safe VIN cache
prevents duplicate inserts across all concurrent button workers.

Architecture:
    ButtonScraper         - One Playwright browser, one button, clicks until exhausted
    VINCache              - Thread-safe in-memory VIN deduplication
    ButtonScrapingCoordinator - Manages all button threads for a single make
    ParallelScrapingOrchestrator - Iterates makes sequentially with stagger
"""

import logging
import time
import random
import os
import json
import threading
from typing import Dict, Set, Optional, List
from datetime import date
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- PLAYWRIGHT REPLACES SELENIUM ---
from playwright.sync_api import sync_playwright

from database import CarDatabase

# --- CONFIGURATION ---

BUTTON_ORDER: List[str] = [
    "autotempest", "hemmings", "carsoup", "carvana", "carmax",
    "autotrader", "ebay", "cars", "other"
]

BUTTON_XPATHS: Dict[str, str] = {
    "autotempest": '//*[@id="te-results"]/section/button',
    "hemmings": '//*[@id="hem-results"]/section/button',
    "cars": '//*[@id="cm-results"]/section/button',
    "carsoup": '//*[@id="cs-results"]/section/button',
    "carvana": '//*[@id="cv-results"]/section/button',
    "carmax": '//*[@id="cx-results"]/section/button',
    "autotrader": '//*[@id="at-results"]/section/button',
    "ebay": '//*[@id="eb-results"]/section/button',
    "other": '//*[@id="ot-results"]/section/button',
}


@dataclass
class ScrapingConfig:
    """Central configuration for all scraping operations"""
    MAKES: List[str] = field(default_factory=lambda: [
        "toyota", "nissan", "ford", "chevrolet", "cadillac",
        "honda", "volvo", "maserati", "porsche", "acura", "lexus", "tesla",
        "kia", "bmw", "mercedes", "hyundai", "infiniti", "dodge", "lotus",
        "suzuki", "mazda", "fiat", "lincoln", "subaru", "gmc",
        "genesis", "jeep", "volkswagen", "landrover", "audi", "ram",
        "chrysler", "jaguar", "ferrari", "lamborghini", "mclaren"
    ])

    INPUT_ZIP: str = "33186"
    INPUT_STATE: str = "country"

    MAX_BUTTON_WORKERS: int = 9
    WORKER_STARTUP_STAGGER: float = 3.0
    MAX_RETRIES: int = 3
    EXHAUSTION_STRIKE_COUNT: int = 3

    MIN_WAIT_AFTER_CLICK: float = 0.6
    MAX_WAIT_AFTER_CLICK: float = 1.2
    MIN_WAIT_BETWEEN_ITERATIONS: float = 2.1
    MAX_WAIT_BETWEEN_ITERATIONS: float = 4.5

    HEADLESS: bool = True
    PAGE_LOAD_TIMEOUT: int = 60

    def get_base_url(self, make: str) -> str:
        return (
            f"https://www.autotempest.com/results"
            f"?localization={self.INPUT_STATE}&zip={self.INPUT_ZIP}&make={make}"
        )


# --- METRICS (Untouched for Log Parity) ---

@dataclass
class ButtonScrapingMetrics:
    button_name: str
    make: str
    clicks: int = 0
    rows_processed: int = 0
    rows_inserted: int = 0
    iterations: int = 0
    strike_count: int = 0
    exhausted: bool = False
    error: Optional[str] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None

    def elapsed(self) -> float:
        return (self.end_time or time.time()) - self.start_time

    def finalize(self):
        self.end_time = time.time()


@dataclass
class MakeScrapingMetrics:
    make: str
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    button_metrics: Dict[str, ButtonScrapingMetrics] = field(default_factory=dict)

    @property
    def total_clicks(self) -> int:
        return sum(m.clicks for m in self.button_metrics.values())

    @property
    def total_rows_processed(self) -> int:
        return sum(m.rows_processed for m in self.button_metrics.values())

    @property
    def total_rows_inserted(self) -> int:
        return sum(m.rows_inserted for m in self.button_metrics.values())

    def elapsed(self) -> float:
        return (self.end_time or time.time()) - self.start_time

    def finalize(self):
        self.end_time = time.time()

    def log_summary(self) -> str:
        lines = [
            f"\n{'=' * 70}",
            f"MAKE: {self.make.upper()}",
            f"{'=' * 70}",
        ]
        for name, bm in self.button_metrics.items():
            status = "EXHAUSTED" if bm.exhausted else ("ERROR" if bm.error else "ACTIVE")
            lines.append(
                f"  {name:14} | clicks={bm.clicks:5} | rows={bm.rows_inserted:6} | "
                f"time={bm.elapsed():.1f}s | {status}"
            )
        lines.extend([
            f"{'-' * 70}",
            f"  TOTALS: {self.total_clicks} clicks | "
            f"{self.total_rows_processed} processed | {self.total_rows_inserted} inserted | "
            f"{self.elapsed():.1f}s",
            f"{'=' * 70}",
        ])
        return "\n".join(lines)


# --- VIN CACHE ---

class VINCache:
    def __init__(self, db: CarDatabase):
        self._lock = threading.Lock()
        self._seen: Dict[str, Dict] = db.get_seen_vins()
        logging.info(f"VINCache initialized with {len(self._seen)} existing VINs")

    def contains(self, vin: str, _loaddate: str = None) -> bool:
        with self._lock:
            return vin in self._seen

    def should_insert(self, vin: str, price: Optional[float], mileage: Optional[int]) -> bool:
        with self._lock:
            if vin not in self._seen:
                return True
            cached = self._seen[vin]
            if cached.get("price") != price or cached.get("mileage") != mileage:
                return True
            return False

    def add(self, vin: str):
        with self._lock:
            if vin not in self._seen:
                self._seen[vin] = {}

    def add_batch(self, rows: List[Dict]):
        with self._lock:
            for r in rows:
                if r.get("vin"):
                    self._seen[r["vin"]] = {"price": r.get("price"), "mileage": r.get("mileage")}

    def size(self) -> int:
        with self._lock:
            return len(self._seen)


# --- DATA EXTRACTION (Untouched for DB Parity) ---

def normalize_price(value) -> Optional[float]:
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    text = str(value).replace("$", "").replace(",", "").strip()
    try:
        return float(text) if text else None
    except ValueError:
        return None


def normalize_mileage(value) -> Optional[int]:
    if value is None: return None
    if isinstance(value, (int, float)): return int(value)
    text = str(value).replace(",", "").strip()
    try:
        return int(float(text)) if text else None
    except ValueError:
        return None


_FIELDS_TO_EXTRACT = [
    "date", "location", "locationCode", "countryCode",
    "pendingSale", "title", "currentBid", "bids", "distance",
    "priceRecentChange", "price", "mileage", "year", "vin",
    "sellerType", "vehicleTitle", "listingType", "vehicleTitleDesc",
    "sourceName", "img",
]

_HISTORY_FIELDS = {"priceHistory", "listingHistory"}


def _parse_history_field(value) -> Optional[List]:
    if value is None: return None
    if isinstance(value, list): return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else None
        except:
            return None
    return None


def extract_rows_from_api(api_data: Dict, make: str) -> List[Dict]:
    rows = []
    items = api_data.get("items") or api_data.get("results") or []
    for item in items:
        row: Dict = {"loaddate": date.today().isoformat(), "scrape_make": make}

        details_short = item.get("detailsShort") or ""
        details_mid = item.get("detailsMid") or ""
        details_long = item.get("detailsLong") or ""
        if details_short or details_mid or details_long:
            row["details"] = f"{details_short}{details_mid}{details_long}"

        row["img"] = item.get("img") or item.get("imgSource") or item.get("imgFallback")

        for hf in _HISTORY_FIELDS:
            row[hf] = _parse_history_field(item.get(hf))

        for f in _FIELDS_TO_EXTRACT:
            if f == "img": continue
            value = item.get(f)
            if f == "price":
                pv = normalize_price(value)
                if pv is not None: row[f] = pv
                continue
            if f == "mileage":
                mv = normalize_mileage(value)
                if mv is not None: row[f] = mv
                continue
            if isinstance(value, dict):
                row[f] = json.dumps(value, ensure_ascii=False)
            else:
                row[f] = value
        rows.append(row)
    return rows


# --- BUTTON SCRAPER (The New Playwright Core) ---

class ButtonScraper:
    def __init__(
            self, make: str, button_name: str, button_xpath: str,
            config: ScrapingConfig, db: CarDatabase, vin_cache: VINCache, worker_tag: str
    ):
        self.make = make
        self.button_name = button_name
        self.button_xpath = button_xpath
        self.config = config
        self.db = db
        self.vin_cache = vin_cache
        self.tag = worker_tag

        # Thread-safe storage for the background wiretap
        self.accumulated_rows: List[Dict] = []
        self._rows_lock = threading.Lock()

        self.p = None
        self.browser = None
        self.context = None
        self.page = None
        self.metrics = ButtonScrapingMetrics(button_name=button_name, make=make)

    def run(self) -> ButtonScrapingMetrics:
        logging.info(f"[{self.tag}] Starting Playwright scraper for '{self.button_name}'")
        try:
            with sync_playwright() as p:
                self.p = p
                self._init_driver()
                self._navigate()
                self._scrape_loop()
        except Exception as e:
            logging.error(f"[{self.tag}] Unexpected error: {e}", exc_info=True)
            self.metrics.error = str(e)
        finally:
            self._cleanup()
        self.metrics.finalize()
        return self.metrics

    def _init_driver(self):
        args = ["--disable-popup-blocking", "--disable-dev-shm-usage", "--no-sandbox"]
        self.browser = self.p.chromium.launch(headless=self.config.HEADLESS, args=args)
        self.context = self.browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}
        )
        self.page = self.context.new_page()

        # SIMPLIFICATION: Block assets for speed, preserving layout rendering
        self.page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media",
                                                                                               "font"] else route.continue_())

        # SIMPLIFICATION: The Network Wiretap (Replaces 40+ lines of CDP parsing)
        self.page.on("response", self._handle_response)

    def _handle_response(self, response):
        if "queue-results" in response.url and response.request.resource_type in ["fetch", "xhr"]:
            if response.status == 200:
                try:
                    data = response.json()
                    rows = extract_rows_from_api(data, self.make)
                    if rows:
                        with self._rows_lock:
                            self.accumulated_rows.extend(rows)
                except Exception:
                    pass

    def _navigate(self):
        url = self.config.get_base_url(self.make)
        logging.info(f"[{self.tag}] Navigating to {url}")
        self.page.goto(url, wait_until="domcontentloaded", timeout=self.config.PAGE_LOAD_TIMEOUT * 1000)
        time.sleep(10)  # Initial aggregator load

    def _cleanup(self):
        if self.browser:
            try:
                self.browser.close()
            except Exception as e:
                logging.warning(f"[{self.tag}] Error closing browser: {e}")
        logging.info(f"[{self.tag}] Cleaned up '{self.button_name}'")

    def _scrape_loop(self):
        iteration = 0
        while True:
            iteration += 1
            self.metrics.iterations = iteration

            self._handle_ribbon()

            clicked = self._attempt_click()
            if not clicked:
                logging.info(f"[{self.tag}] Button no longer clickable/visible. Exhausted.")
                self.metrics.exhausted = True
                break

            self.metrics.clicks += 1

            # Give wiretap time to catch the asynchronous background payloads
            time.sleep(2.5)

            # Atomically pull the intercepted cars
            with self._rows_lock:
                api_rows = list(self.accumulated_rows)
                self.accumulated_rows.clear()

            if api_rows:
                inserted = self.db.insert_rows(api_rows, vin_cache=self.vin_cache)
                self.vin_cache.add_batch(api_rows)
                self.metrics.rows_processed += len(api_rows)
                self.metrics.rows_inserted += inserted
                self.metrics.strike_count = 0
                logging.info(f"[{self.tag}] +{len(api_rows)} rows, {inserted} inserted")
                print(f"[{self.make}/{self.button_name}] iter={iteration} rows={len(api_rows)} inserted={inserted}")
            else:
                self.metrics.strike_count += 1
                logging.info(
                    f"[{self.tag}] 0 rows (strike {self.metrics.strike_count}/{self.config.EXHAUSTION_STRIKE_COUNT})")
                if self.metrics.strike_count >= self.config.EXHAUSTION_STRIKE_COUNT:
                    logging.info(f"[{self.tag}] Exhausted after {self.config.EXHAUSTION_STRIKE_COUNT} zero-row strikes")
                    self.metrics.exhausted = True
                    break

            time.sleep(random.uniform(self.config.MIN_WAIT_BETWEEN_ITERATIONS, self.config.MAX_WAIT_BETWEEN_ITERATIONS))

    def _attempt_click(self) -> bool:
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                # SIMPLIFICATION: Playwright's native locator replaces complex ActionChains
                button = self.page.locator(f"xpath={self.button_xpath}")
                button.wait_for(state="visible", timeout=4000)
                button.scroll_into_view_if_needed()
                time.sleep(random.uniform(self.config.MIN_WAIT_AFTER_CLICK, self.config.MAX_WAIT_AFTER_CLICK))
                button.click(timeout=5000)
                return True
            except Exception as e:
                logging.debug(f"[{self.tag}] Click attempt {attempt} failed: {e}")
                time.sleep(1)
        return False

    def _handle_ribbon(self) -> bool:
        try:
            ribbon_dismiss = self.page.locator('xpath=//*[@id="cta-dismiss"]/i')
            if ribbon_dismiss.is_visible():
                ribbon_dismiss.click(timeout=2000)
                self.page.wait_for_timeout(1000)
                logging.info(f"[{self.tag}] Ribbon dismissed")
                return True
        except Exception:
            pass
        return False


# --- COORDINATOR & ORCHESTRATOR (Untouched) ---

class ButtonScrapingCoordinator:
    def __init__(self, make: str, config: ScrapingConfig, db_path: str, output_dir: str):
        self.make = make
        self.config = config
        self.db_path = db_path
        self.output_dir = output_dir
        self.metrics = MakeScrapingMetrics(make=make)

    def run(self) -> MakeScrapingMetrics:
        db = CarDatabase(self.db_path, thread_safe=True)
        vin_cache = VINCache(db)

        ordered_buttons = [
            (name, BUTTON_XPATHS[name]) for name in BUTTON_ORDER
            if name in BUTTON_XPATHS
        ]

        logging.info(
            f"[{self.make.upper()}] Launching {len(ordered_buttons)} button workers (max {self.config.MAX_BUTTON_WORKERS} concurrent)")

        futures = {}
        with ThreadPoolExecutor(max_workers=self.config.MAX_BUTTON_WORKERS, thread_name_prefix=f"{self.make}") as pool:
            for idx, (btn_name, btn_xpath) in enumerate(ordered_buttons):
                tag = f"{self.make}/{btn_name}"
                scraper = ButtonScraper(
                    make=self.make, button_name=btn_name, button_xpath=btn_xpath,
                    config=self.config, db=db, vin_cache=vin_cache, worker_tag=tag,
                )
                future = pool.submit(scraper.run)
                futures[future] = btn_name
                if idx < len(ordered_buttons) - 1:
                    time.sleep(random.uniform(1.5, 3.0))

            for future in as_completed(futures):
                btn_name = futures[future]
                try:
                    bm = future.result()
                    self.metrics.button_metrics[btn_name] = bm
                    status = "EXHAUSTED" if bm.exhausted else "ERROR"
                    logging.info(
                        f"[{self.make}/{btn_name}] Finished: {status} | clicks={bm.clicks} rows={bm.rows_inserted} time={bm.elapsed():.1f}s")
                except Exception as e:
                    logging.error(f"[{self.make}/{btn_name}] Thread error: {e}")
                    err_metrics = ButtonScrapingMetrics(button_name=btn_name, make=self.make, error=str(e))
                    err_metrics.finalize()
                    self.metrics.button_metrics[btn_name] = err_metrics

        db.close()
        self.metrics.finalize()
        return self.metrics


class ParallelScrapingOrchestrator:
    def __init__(self, config: ScrapingConfig = None):
        self.config = config or ScrapingConfig()
        self.output_directory = os.path.join(os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT")
        os.makedirs(self.output_directory, exist_ok=True)
        self.db_path = os.path.join(self.output_directory, "CAR_DATA.db")
        self.metrics_by_make: Dict[str, MakeScrapingMetrics] = {}
        self._setup_logging()

    def _setup_logging(self):
        log_path = os.path.join(self.output_directory, f"parallel_scraping_{date.today()}.log")
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        logging.basicConfig(
            filename=log_path, level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s", force=True,
        )
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        logging.getLogger("").addHandler(console)

    def run(self):
        logging.info("=" * 70)
        logging.info("Starting Per-Button Parallel Scraping")
        logging.info("=" * 70)
        logging.info(f"Makes: {', '.join(self.config.MAKES)}")
        logging.info(f"Buttons per make: {len(BUTTON_ORDER)}")
        logging.info(f"Max concurrent buttons: {self.config.MAX_BUTTON_WORKERS}")

        start_time = time.time()
        try:
            for idx, make in enumerate(self.config.MAKES):
                if idx > 0:
                    stagger = self.config.WORKER_STARTUP_STAGGER
                    logging.info(f"Staggering next make by {stagger}s...")
                    time.sleep(stagger)

                logging.info(f"\n[ORCHESTRATOR] Make {idx + 1}/{len(self.config.MAKES)}: {make.upper()}")

                coordinator = ButtonScrapingCoordinator(
                    make=make, config=self.config, db_path=self.db_path, output_dir=self.output_directory,
                )
                metrics = coordinator.run()
                self.metrics_by_make[make] = metrics
                print(metrics.log_summary())

            self._print_aggregate_summary(start_time)

        except KeyboardInterrupt:
            logging.info("Scraping interrupted by user")
            print("\nScraping interrupted by user")
        except Exception as e:
            logging.error(f"Orchestrator error: {e}", exc_info=True)
        finally:
            logging.info("Orchestrator finished")

    def _print_aggregate_summary(self, start_time: float):
        elapsed = time.time() - start_time
        total_clicks = sum(m.total_clicks for m in self.metrics_by_make.values())
        total_rows = sum(m.total_rows_processed for m in self.metrics_by_make.values())
        total_ins = sum(m.total_rows_inserted for m in self.metrics_by_make.values())

        lines = [
            "\n" + "=" * 70,
            "AGGREGATE SCRAPING SUMMARY",
            "=" * 70,
            f"Makes processed: {len(self.metrics_by_make)}",
            f"Elapsed: {elapsed:.1f}s ({elapsed / 60:.1f}m)",
            "",
        ]
        for make, m in self.metrics_by_make.items():
            lines.append(
                f"  {make.upper():14} | clicks={m.total_clicks:5} | "
                f"rows={m.total_rows_processed:6} | inserted={m.total_rows_inserted:6} | "
                f"time={m.elapsed():.1f}s"
            )
        lines.extend([
            "=" * 70,
            f"TOTALS: {total_clicks} clicks | {total_rows} processed | {total_ins} inserted",
            "=" * 70,
        ])
        summary = "\n".join(lines)
        print(summary)
        logging.info(summary)


def main():
    print("Running data acquisition pipeline...")
    config = ScrapingConfig()
    orchestrator = ParallelScrapingOrchestrator(config)
    orchestrator.run()


if __name__ == "__main__":
    main()