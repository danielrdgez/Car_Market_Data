"""
Global Queue Parallel Car Data Acquisition Module (Playwright Edition)
=====================================================================
This architecture utilizes a Global Task Queue. It generates a master list of
ALL (Make, Button) combinations and feeds them into a single ThreadPoolExecutor.
This guarantees that the maximum number of concurrent browsers are ALWAYS running,
eliminating "tail-end latency" where workers wait for a slow make to finish.
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

    # --- GLOBAL CONCURRENCY SETTING ---
    # This dictates the exact number of browsers that will ALWAYS be running across all makes.
    MAX_GLOBAL_CONCURRENT_BROWSERS: int = 9

    # Safely staggers browser launches so your CPU/Network doesn't crash from spiking
    GLOBAL_STARTUP_STAGGER: float = 2.0

    MAX_RETRIES: int = 3
    EXHAUSTION_STRIKE_COUNT: int = 3

    MIN_WAIT_AFTER_CLICK: float = 0.6
    MAX_WAIT_AFTER_CLICK: float = 1.2
    MIN_WAIT_BETWEEN_ITERATIONS: float = 2.1
    MAX_WAIT_BETWEEN_ITERATIONS: float = 4.5

    HEADLESS: bool = True  # Set to True for optimal background/server running
    PAGE_LOAD_TIMEOUT: int = 60

    def get_base_url(self, make: str) -> str:
        return (
            f"https://www.autotempest.com/results"
            f"?localization={self.INPUT_STATE}&zip={self.INPUT_ZIP}&make={make}"
        )


# --- METRICS ---

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

    def add_batch(self, rows: List[Dict]):
        with self._lock:
            for r in rows:
                if r.get("vin"):
                    self._seen[r["vin"]] = {"price": r.get("price"), "mileage": r.get("mileage")}

    def size(self) -> int:
        with self._lock:
            return len(self._seen)


# --- DATA EXTRACTION ---

def normalize_price(value) -> Optional[float]:
    if value is None: return None
    if isinstance(value, (int, float)): return float(value)
    text = str(value).replace("$", "").replace(",", "").strip()
    try: return float(text) if text else None
    except ValueError: return None

def normalize_mileage(value) -> Optional[int]:
    if value is None: return None
    if isinstance(value, (int, float)): return int(value)
    text = str(value).replace(",", "").strip()
    try: return int(float(text)) if text else None
    except ValueError: return None

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
        except: return None
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


# --- BUTTON SCRAPER ---

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

        self.accumulated_rows: List[Dict] = []
        self._rows_lock = threading.Lock()

        self.p = None
        self.browser = None
        self.context = None
        self.page = None
        self.metrics = ButtonScrapingMetrics(button_name=button_name, make=make)

    def run(self) -> ButtonScrapingMetrics:
        try:
            with sync_playwright() as p:
                self.p = p
                self._init_driver()
                self._navigate()
                self._scrape_loop()
        except Exception as e:
            logging.error(f"[{self.tag}] Unexpected error: {e}")
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

        self.page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "media", "font"] else route.continue_())
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
        self.page.goto(url, wait_until="domcontentloaded", timeout=self.config.PAGE_LOAD_TIMEOUT * 1000)
        time.sleep(10)

    def _cleanup(self):
        if self.browser:
            try:
                self.browser.close()
            except Exception:
                pass

    def _scrape_loop(self):
        iteration = 0
        while True:
            iteration += 1
            self.metrics.iterations = iteration

            self._handle_ribbon()

            clicked = self._attempt_click()
            if not clicked:
                self.metrics.exhausted = True
                break

            self.metrics.clicks += 1
            time.sleep(2.5)

            with self._rows_lock:
                api_rows = list(self.accumulated_rows)
                self.accumulated_rows.clear()

            if api_rows:
                inserted = self.db.insert_rows(api_rows, vin_cache=self.vin_cache)
                self.vin_cache.add_batch(api_rows)
                self.metrics.rows_processed += len(api_rows)
                self.metrics.rows_inserted += inserted
                self.metrics.strike_count = 0
                print(f"[{self.tag}] iter={iteration} rows={len(api_rows)} inserted={inserted}")
            else:
                self.metrics.strike_count += 1
                if self.metrics.strike_count >= self.config.EXHAUSTION_STRIKE_COUNT:
                    self.metrics.exhausted = True
                    break

            time.sleep(random.uniform(self.config.MIN_WAIT_BETWEEN_ITERATIONS, self.config.MAX_WAIT_BETWEEN_ITERATIONS))

    def _attempt_click(self) -> bool:
        for attempt in range(1, self.config.MAX_RETRIES + 1):
            try:
                button = self.page.locator(f"xpath={self.button_xpath}")
                button.wait_for(state="visible", timeout=4000)
                button.scroll_into_view_if_needed()
                time.sleep(random.uniform(self.config.MIN_WAIT_AFTER_CLICK, self.config.MAX_WAIT_AFTER_CLICK))
                button.click(timeout=5000)
                return True
            except Exception:
                time.sleep(1)
        return False

    def _handle_ribbon(self) -> bool:
        try:
            ribbon_dismiss = self.page.locator('xpath=//*[@id="cta-dismiss"]/i')
            if ribbon_dismiss.is_visible():
                ribbon_dismiss.click(timeout=2000)
                self.page.wait_for_timeout(1000)
                return True
        except Exception:
            pass
        return False


# --- GLOBAL TASK ORCHESTRATOR ---

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
        logging.info("Starting GLOBAL QUEUE Parallel Scraping (Playwright)")
        logging.info("=" * 70)

        db = CarDatabase(self.db_path, thread_safe=True)
        vin_cache = VINCache(db)

        # 1. GENERATE THE MASTER QUEUE
        master_queue = []
        for make in self.config.MAKES:
            self.metrics_by_make[make] = MakeScrapingMetrics(make=make)
            for btn_name in BUTTON_ORDER:
                if btn_name in BUTTON_XPATHS:
                    master_queue.append((make, btn_name, BUTTON_XPATHS[btn_name]))

        total_tasks = len(master_queue)
        completed_tasks = 0

        logging.info(f"Total Makes: {len(self.config.MAKES)}")
        logging.info(f"Total Buttons: {len(BUTTON_ORDER)}")
        logging.info(f"Master Queue Size: {total_tasks} isolated tasks generated.")
        logging.info(f"Max Concurrent Browsers: {self.config.MAX_GLOBAL_CONCURRENT_BROWSERS}")

        start_time = time.time()

        # 2. CREATE A STARTUP LOCK
        # This prevents 9 browsers from trying to allocate RAM/CPU at the exact same millisecond.
        startup_lock = threading.Lock()

        def worker_task(task_info):
            make, btn_name, btn_xpath = task_info
            tag = f"{make}/{btn_name}"

            # Acquire lock -> Sleep 2 seconds -> Release lock -> Start Browser
            with startup_lock:
                time.sleep(self.config.GLOBAL_STARTUP_STAGGER)

            scraper = ButtonScraper(
                make=make, button_name=btn_name, button_xpath=btn_xpath,
                config=self.config, db=db, vin_cache=vin_cache, worker_tag=tag,
            )
            return make, btn_name, scraper.run()

        # 3. FEED THE MASTER QUEUE TO THE POOL
        try:
            with ThreadPoolExecutor(max_workers=self.config.MAX_GLOBAL_CONCURRENT_BROWSERS) as pool:
                futures = {pool.submit(worker_task, task): task for task in master_queue}

                for future in as_completed(futures):
                    make, btn_name, _ = futures[future]
                    completed_tasks += 1
                    try:
                        res_make, res_btn, bm = future.result()

                        # Save metrics for this specific button back to its parent make
                        self.metrics_by_make[res_make].button_metrics[res_btn] = bm
                        status = "EXHAUSTED" if bm.exhausted else "ERROR"

                        progress = f"[{completed_tasks}/{total_tasks}]"
                        logging.info(f"{progress} [{res_make.upper()}/{res_btn}] Finished: {status} | clicks={bm.clicks} rows={bm.rows_inserted} time={bm.elapsed():.1f}s")

                    except Exception as e:
                        logging.error(f"[{make}/{btn_name}] Task completely failed: {e}")
                        err_metrics = ButtonScrapingMetrics(button_name=btn_name, make=make, error=str(e))
                        err_metrics.finalize()
                        self.metrics_by_make[make].button_metrics[btn_name] = err_metrics

            self._print_aggregate_summary(start_time)

        except KeyboardInterrupt:
            logging.info("Scraping interrupted by user")
            print("\nScraping interrupted by user")
        finally:
            db.close()
            logging.info("Orchestrator finished")

    def _print_aggregate_summary(self, start_time: float):
        elapsed = time.time() - start_time
        total_clicks = sum(m.total_clicks for m in self.metrics_by_make.values())
        total_rows = sum(m.total_rows_processed for m in self.metrics_by_make.values())
        total_ins = sum(m.total_rows_inserted for m in self.metrics_by_make.values())

        lines = [
            "\n" + "=" * 70,
            "AGGREGATE GLOBAL SCRAPING SUMMARY",
            "=" * 70,
            f"Makes fully processed: {len(self.metrics_by_make)}",
            f"Elapsed Time: {elapsed:.1f}s ({elapsed/60:.1f}m)",
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
    print("Running GLOBAL Playwright Data Acquisition Pipeline...")
    config = ScrapingConfig()
    orchestrator = ParallelScrapingOrchestrator(config)
    orchestrator.run()

if __name__ == "__main__":
    main()