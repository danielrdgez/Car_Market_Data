---
applyTo: "**/*.py"
---

# Python & Scraper Guidelines: Car_Market_Data

## 1. Global Logic & Strategy
**Goal:** Scrape AutoTempest by acting as a "Human Browser" while intercepting backend JSON data.
**Core Rule:** NEVER use standard Selenium methods if a "Stealth" or "API" alternative exists.

### The "Golden Path" for Execution:
1.  **Initialize** with `selenium-stealth` (hides bot flags).
2.  **Navigate** using random delays and human-like mouse movements.
3.  **Intercept** the `queue-results` JSON response via CDP (Network logs) instead of scraping HTML.
4.  **Save** data incrementally to prevent loss during crashes.

---

## 2. Selenium Configuration (The "Stealth" Setup)
**Target:** Bypass WAFs (Cloudflare/Akamai) by removing `navigator.webdriver` flags.

**Required Code Pattern:**
```python
from selenium import webdriver
from selenium_stealth import stealth

def setup_driver(headless=False):
    options = webdriver.ChromeOptions()
    
    # CRITICAL: Disable internal automation flags
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # Enable Performance Logging (Required for API Interception)
    options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
    
    if headless:
        # If headless, you MUST manually set a User-Agent
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
    return driver