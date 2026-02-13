# Global Project Instructions: Car_Market_Data

## Project Overview
This project aggregates vehicle market data from AutoTempest and enriches it with official NHTSA specifications. It uses a sophisticated "API Interception" strategy to capture data without parsing HTML.

## Core Principles
1. **Stealth First:** All automation MUST prioritize avoiding detection.
    - **Never** use standard Selenium without `selenium-stealth`.
    - **Never** use fixed `time.sleep()`; always use randomized intervals (e.g., `random.uniform(2.1, 4.5)`).
    - **Never** announce the `navigator.webdriver` property.
    - **Never** use emojis in the code when creating new code always keep it professional and neat with minimal comments only when necessary and the code readable like a expert data scientist.
    - **Never** create unnecesary markdown files and dont use emojis in markdown files keep the documentation professional and only add to the project_summary and the readme markdown documents
    - Focus on legibility and concise code write like you are an expert datascientist and a staff level software engineer.
2. **API Interception > HTML Parsing:**
    - Do not parse HTML for car data.
    - ALWAYS prefer intercepting network traffic (CDP Performance Logs) to capture raw JSON from `queue-results` endpoints.
3. **Data Integrity:**
    - Prices must be numeric (no symbols).
    - VINs must be validated before enrichment.
    - All outputs must be sent to CAR_DATA.db database periodically in a queue.

## Tech Stack
- **Language:** Python 3.10+
- **Browser:** Selenium (Chrome) + `selenium-stealth`
- **Data:** Pandas, sqlite3, json
- **Enrichment:** NHTSA vPIC API