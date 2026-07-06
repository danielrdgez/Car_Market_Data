# Global Project Instructions: Automotive Market ML Capstone

## Project Overview

This repository is a master's capstone data science project focused on current new and used vehicle market pricing. It aggregates vehicle listings from AutoTempest-style result pages, enriches VINs with official NHTSA data, integrates YouTube consumer-comment sentiment, and trains machine learning models for price prediction and depreciation forecasting.

The active scraper is `DataPipeline/Playwright_test.py`. It uses Playwright response interception to capture structured `queue-results` JSON and persist it to SQLite. `DataPipeline/DataAquisition.py` is the legacy/reference Selenium CDP scraper.

## Core Principles

1. **Network Interception Over HTML Parsing**
   - Do not parse listing cards from HTML when structured `queue-results` network responses are available.
   - Preserve Playwright response interception in `DataPipeline/Playwright_test.py`.
   - Keep the Selenium CDP fallback in `DataPipeline/DataAquisition.py` working unless the user asks to retire it.

2. **Responsible Scraping**
   - Use bounded concurrency, randomized delays, and conservative retry behavior.
   - Do not make scraper behavior more aggressive without clear stability safeguards.
   - Persist data incrementally to `CAR_DATA_OUTPUT/CAR_DATA.db`.

3. **Data Integrity**
   - Prices and mileage must be normalized to numeric values before analysis or modeling.
   - VINs must be validated before enrichment.
   - Keep all NHTSA-enriched columns prefixed with `nhtsa_`.
   - Preserve raw snapshots and normalized history tables.

4. **Research-Grade Modeling**
   - Tie analysis to the capstone questions: safety/depreciation, high-dimensional price prediction, depreciation forecasting, NLP/sentiment lift, and segment robustness.
   - Avoid target leakage and preserve VIN-safe validation.
   - Use bounded database reads by default; make full-database runs intentional.
   - Report assumptions, row counts, split strategy, metrics, caveats, and research rationale.

5. **Professional Style**
   - Keep code and markdown professional and emoji-free.
   - Use minimal comments that explain non-obvious logic.
   - Prefer legible, concise implementation over broad refactors.

## Documentation and Dependency Hygiene

- Do not create extra markdown files unless explicitly requested.
- Update existing docs first: `README.md`, `PROJECT_SUMMARY.md`, root `AGENTS.md`, and relevant `.github/*.md`.
- When introducing new packages, update `requirements.txt` in the same change.

## Data Cleaning Direction

- Prefer Polars as the default dataframe engine for new data-cleaning work.
- Keep Pandas where existing modeling, notebook, or library code already depends on it.
- Preserve behavior when touching existing Pandas code and migrate incrementally.

## Tech Stack

- Language: Python 3.10+
- Active browser automation: Playwright
- Legacy/reference browser automation: Selenium Chrome with `selenium-stealth`
- Data: SQLite, Polars, Pandas, NumPy
- Visualization: Plotly for Python EDA, ggplot2 for R EDA
- Modeling: scikit-learn, LightGBM, category encoders, joblib
- NLP/sentiment: YouTube Data API, transformers, torch
- Enrichment: NHTSA vPIC, SafetyRatings, Recalls, Complaints APIs
