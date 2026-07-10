# GitHub Agent Guide

This file mirrors the root `AGENTS.md` for tools that read instructions from `.github/`. If the two files ever disagree, update both and treat the root `AGENTS.md` as the source of truth.

## Mission-Critical Context

- This is a master's capstone data science project for automotive market pricing, depreciation, NHTSA enrichment, and consumer sentiment.
- The active scraper is `DataPipeline/Playwright_test.py`.
- The current data flow is `DataPipeline/Playwright_test.py` -> `DataPipeline/database.py` -> `DataPipeline/NHTSA_enrichment.py` -> `DataPipeline/DataCleaning.py` -> EDA and ML.
- `DataPipeline/DataAquisition.py` is the legacy/reference Selenium CDP scraper. Keep it working, but do not treat it as the default path unless asked.
- Preserve the key acquisition design: intercept structured `queue-results` network responses and persist them incrementally. Do not switch to HTML parsing for vehicle data.

## Architecture To Learn First

- `Playwright_test.py` builds a global queue of `(make, source button)` tasks so browser workers stay busy across all makes.
- Each `ButtonScraper` owns its own Playwright browser context and intercepts `queue-results` fetch/XHR responses.
- Button completion is behavioral: stop after `EXHAUSTION_STRIKE_COUNT` consecutive zero-row responses.
- Dedup is two-layered: in-memory `VINCache` plus DB-sourced `get_seen_vins()`.
- Acquisition writes use `CarDatabase(thread_safe=True)` with thread-local SQLite connections and a shared write lock.
- `DataCleaning.py` builds `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db` with normalized data, contextual price-outlier filtering, durable trim features, and modeling indexes.
- `Price_ML_Models.py` trains leakage-aware current-price models.
- `Time_Series_Price.py` trains cohort-level depreciation forecasts.

## Runbook Commands

```powershell
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\Playwright_test.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py
python -m unittest tests\test_ml_upgrade.py
streamlit run streamlit_app.py
```

Use this before scheduled ingestion:

```powershell
run_pipeline_scheduler.bat --dry-run
```

## Coding Rules Specific To This Repo

- Preserve Playwright response interception for `queue-results`; do not scrape listing HTML when structured network data is available.
- Use randomized waits and bounded concurrency for scraper stability.
- Keep ingestion values normalized with `normalize_price` and `normalize_mileage`.
- Validate VINs before enrichment with `_is_valid_vin`.
- Keep all NHTSA-derived fields prefixed with `nhtsa_`.
- Prefer incremental persistence over large in-memory accumulation.
- For data cleaning work, prefer Polars by default and keep Pandas where modeling/notebook code already depends on it.
- For price outliers, prefer robust cohort-aware checks over only global cutoffs; preserve title-derived trim fields and NHTSA trim provenance for modeling.
- Preserve `nhtsa_BasePrice` provenance; missing cleaned base prices can be filled from earliest cleaned price history, then earliest cleaned listing history.
- Keep depreciation forecasts cohort-level and monthly out to five years by default, not VIN-level or fixed 30/60/90-day buckets.
- Preserve direct script execution with `if __name__ == "__main__": main()`.
- For depreciation model changes, preserve the monthly cohort forecast and backtesting output contracts: `cohort_future_forecasts.csv`, `cohort_backtesting_results.csv`, and `cohort_backtesting_kpis.csv`.
- Keep code and markdown professional, concise, and emoji-free.

## Research and Modeling Standards

- Tie EDA and feature engineering to the capstone research questions: safety/depreciation, high-dimensional price prediction, cohort depreciation forecasting, NLP/sentiment lift, and segment robustness.
- Avoid target leakage. Do not train on `price`, `price_band`, future prices, or answer-derived features.
- Preserve VIN-safe train/test validation for current-price modeling.
- Prefer temporal validation when enough dates exist.
- Keep full-database runs opt-in; default to bounded samples for development.
- When adding research claims or new techniques, verify against current primary sources, official documentation, or peer-reviewed papers.

## Documentation and Dependency Hygiene

- Update `README.md`, `PROJECT_SUMMARY.md`, root `AGENTS.md`, and relevant `.github` instructions when workflows, schemas, or modeling practices change.
- Do not create extra markdown files unless requested.
- When adding new packages, update `requirements.txt` in the same change.
- Prefer the repo-local `.venv` when launching pipeline or scraper commands; `py -3` may resolve to a different interpreter on Windows.
- Playwright setup has two parts: the Python package from `requirements.txt` and the browser binaries from `python -m playwright install`.

## Gotchas

- `Playwright_test.py` is the current scraper even though its filename sounds experimental.
- `DataAquisition.py` is misspelled historically and remains the Selenium reference/fallback path.
- Some utility and EDA scripts still contain absolute Windows paths; improve portability when touching those files.
- YouTube ingestion requires `YOUTUBE_API_KEY` or `GOOGLE_API_KEY`.
- Large database scans can be expensive. Use sample-size defaults unless a full capstone run is intentional.
