# Global Project Instructions: Car_Market_Data

## Project Overview
This project aggregates vehicle market data from AutoTempest and enriches it with official NHTSA specifications. It uses a sophisticated "API Interception" strategy to capture data without parsing HTML.

## Core Principles
1. **Stealth First:** All automation MUST prioritize avoiding detection.
    - **Never** use standard Selenium without `selenium-stealth`.
    - **Never** use fixed `time.sleep()`; always use randomized intervals (e.g., `random.uniform(2.1, 4.5)`).
    - **Never** announce the `navigator.webdriver` property.
2. **API Interception > HTML Parsing:**
    - Do not parse HTML for car data.
    - ALWAYS prefer intercepting network traffic (CDP Performance Logs) to capture raw JSON from `queue-results` endpoints.
3. **Data Integrity:**
    - Prices must be numeric (no symbols).
    - VINs must be validated before enrichment.
    - All output CSVs must use `mode='a'` (append) to prevent data loss on crash.

## Tech Stack
- **Language:** Python 3.10+
- **Browser:** Selenium (Chrome) + `selenium-stealth`
- **Data:** Pandas, JSON, CSV
- **Enrichment:** NHTSA vPIC API