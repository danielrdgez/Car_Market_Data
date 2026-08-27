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
- Enrichment: NHTSA vPIC, SafetyRatings, Recalls, Complaints APIs, and official bulk datasets. Normalized NHTSA history is stored in `CAR_DATA_OUTPUT/CAR_DATA_NHTSA.db`; `CAR_DATA.db` retains the compatibility projection.

## NHTSA Implementation Rules

- Use vPIC batches of no more than 50 VINs with the documented lowercase `data` form field and model-year hints when available.
- Perform both Safety Ratings calls: model-year/make/model variant lookup followed by a `VehicleId` detail lookup for every variant.
- Persist all available source fields in typed columns or normalized field/value tables, but never duplicate full API responses, request payloads, record JSON, or raw bulk-row blobs; compatibility columns are a derived projection only.
- Resolve identity per field with NHTSA first and listing data as fallback, recording source and conflicts.
- Use conservative rate limiting, retries, resumable cache reads, and incremental SQLite writes.
- Use --backup-path for a one-time live SQLite backup before the first migration of an existing primary database; never overwrite an existing backup.
- Use --backfill-legacy as the explicit full-history refresh alias when the goal is to reprocess every historical VIN with current mappings.
- Update README.md, PROJECT_SUMMARY.md, AGENTS.md, and relevant `.github` guidance whenever this workflow or its schema changes.
