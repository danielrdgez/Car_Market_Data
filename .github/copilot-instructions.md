# Global Project Instructions: Car_Market_Data

## Project Overview
This project aggregates vehicle market data from AutoTempest and enriches it with official NHTSA specifications. It uses a sophisticated "API Interception" strategy to capture data without parsing HTML.

## Core Principles
1. **Stealth First:** All automation MUST prioritize avoiding detection.
    - **Never** use standard Selenium without `selenium-stealth`.
    - **Never** use fixed `time.sleep()`; always use randomized intervals (e.g., `random.uniform(2.1, 4.5)`).
    - **Never** announce the `navigator.webdriver` property.
    - Keep code and markdown professional and emoji-free.
    - Use minimal comments: explain only non-obvious logic.
    - Focus on legibility and concise implementation style.
2. **API Interception > HTML Parsing:**
    - Do not parse HTML for car data.
    - ALWAYS prefer intercepting network traffic (CDP Performance Logs) to capture raw JSON from `queue-results` endpoints.
3. **Data Integrity:**
    - Prices must be numeric (no symbols).
    - VINs must be validated before enrichment.
    - All outputs must be sent to CAR_DATA.db database periodically in a queue.
    - Keep all NHTSA-enriched columns prefixed with `nhtsa_`.

## Documentation And Dependency Hygiene
- Do not create extra markdown files unless explicitly requested.
- Update existing docs first: `README.md`, `PROJECT_SUMMARY.md`, and `.github/*.md`.
- When introducing new packages, update `requirements.txt` in the same change.

## Data Cleaning Direction
- Prefer `polars` as the default dataframe engine for new data-cleaning work.
- If existing `pandas` code is touched, keep behavior stable and migrate incrementally.

## Tech Stack
- **Language:** Python 3.10+
- **Browser:** Selenium (Chrome) + `selenium-stealth`
- **Data:** Polars/Pandas, sqlite3, json
- **Enrichment:** NHTSA vPIC API