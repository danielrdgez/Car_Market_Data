# Project Summary

Last updated: July 5, 2026

## Executive Overview

This repository implements a data science capstone project for automotive market analysis. It combines web-scraped vehicle listings, official NHTSA vehicle metadata, YouTube consumer-comment data, exploratory analysis, and machine learning to study current vehicle pricing and depreciation in the new and used car market.

The project is designed around a research-grade workflow:

1. Capture market listings as structured JSON from network responses.
2. Persist raw listing snapshots and historical price movement in SQLite.
3. Enrich VINs with NHTSA specifications, safety ratings, recall counts, and complaint signals.
4. Clean and normalize the resulting relational data into an analysis database.
5. Engineer leakage-aware features for current-price prediction and depreciation forecasting.
6. Add consumer sentiment features where the YouTube comment pipeline has sufficient support.
7. Produce reproducible notebooks, model reports, and model artifacts for capstone evaluation.

## Research Questions

1. Safety and depreciation: do active safety systems, safety ratings, recalls, or complaints explain differences in resale value and depreciation?
2. High-dimensional price prediction: how much predictive lift comes from rich NHTSA vehicle attributes beyond age, mileage, location, and listing metadata?
3. Depreciation forecasting: can cohort-level time-series models forecast future median price changes for make, model, model year, and trim-like groups?
4. Sentiment integration: does consumer sentiment from YouTube reviews and comments add measurable signal to vehicle pricing models?
5. Market robustness: how do model results change across price bands, high-value vehicles, makes, model years, and data collection windows?

## Architecture

```text
Acquisition
  AutoTempest result pages
  -> queue-results JSON interception
  -> SQLite raw tables

Enrichment
  Raw VINs
  -> NHTSA vPIC, SafetyRatings, Recalls, Complaints
  -> nhtsa_enrichment

Cleaning
  CAR_DATA.db
  -> Polars normalization, filters, indexes
  -> CAR_DATA_CLEANED.db

Analysis and ML
  Cleaned listings, history, NHTSA, sentiment
  -> EDA notebooks and scripts
  -> current-price models
  -> cohort depreciation forecasts
```

## Core Components

### Data Acquisition

`DataPipeline/Playwright_test.py` is the current active scraper. It uses Playwright, a global task queue, and response interception for `queue-results` network calls. The global queue creates a master list of `(make, source button)` tasks and feeds those tasks into a shared worker pool so the configured number of browser workers stays busy across all makes.

Key classes and functions:

- `ScrapingConfig`: make list, ZIP/localization, global browser concurrency, delays, retry limits, and headless behavior.
- `VINCache`: thread-safe in-memory deduplication backed by latest database state.
- `ButtonScraper`: one Playwright browser context per source-button task.
- `ParallelScrapingOrchestrator`: global queue orchestration across all makes and source buttons.
- `extract_rows_from_api`: converts intercepted JSON into normalized row dictionaries.

`DataPipeline/DataAquisition.py` remains a legacy/reference Selenium CDP scraper. It is useful for comparing behavior, preserving the original stealth Selenium approach, and recovering patterns if the Playwright path regresses. Do not treat it as the primary scraper unless the user explicitly asks to switch back.

### Persistence

`DataPipeline/database.py` owns the SQLite schema and insert behavior.

Main tables:

- `listings`: listing snapshots keyed by `(vin, loaddate)`.
- `price_history`: normalized price history entries from listing payloads.
- `listing_history`: normalized listing history entries.
- `nhtsa_enrichment`: VIN-level NHTSA specs, safety, recall, and complaint fields.
- `youtube_comments_sentiment`: YouTube comment ingestion output.

Important behavior:

- Acquisition writes are incremental.
- Thread-safe acquisition uses thread-local SQLite connections and a shared write lock.
- History tables use `INSERT OR IGNORE` with uniqueness constraints.
- NHTSA inserts use batch `INSERT OR REPLACE`.

### NHTSA Enrichment

`DataPipeline/NHTSA_enrichment.py` enriches VINs that are present in `listings` but missing from `nhtsa_enrichment`.

Sources:

- vPIC VIN decode fields.
- NHTSA SafetyRatings API.
- NHTSA Recalls API.
- NHTSA Complaints API.

The enricher batches VIN decode requests up to 50 VINs, uses worker threads, caches make/model/year safety, recall, and complaint lookups, and prefixes all derived columns with `nhtsa_`.

### Cleaning

`DataPipeline/DataCleaning.py` builds `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db` from `CAR_DATA.db`.

Current cleaning choices:

- Uses Polars for table reads, type normalization, filtering, and output.
- Keeps predictive listing fields such as title, location, source, seller type, listing type, vehicle title, and price-change flags.
- Drops obviously invalid price rows outside the configured range.
- Normalizes date and numeric columns.
- Filters NHTSA rows to supported makes and valid make/model/model year values.
- Creates indexes for modeling and time-series reads.

The cleaned database is the preferred input for EDA and modeling.

### Sentiment and Aspect-Based NLP

`DataPipeline/SentimentAnalysis.py` uses the YouTube Data API to collect comments from configured videos or playlists. It now persists playlist discovery plus per-video fetch state in `CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db`, prioritizes unseen videos first, and refreshes completed videos on a bounded schedule instead of restarting every playlist from the top on each run.

`DataPipeline/absa_pipeline.py` performs aspect-based sentiment analysis:

- Extracts vehicle entities from video titles.
- Cleans comments and filters spam-like or low-information messages.
- Uses zero-shot classification for reliability, value, performance, and comfort aspects.
- Splits longer comments into smaller chunks before scoring to reduce mixed-topic dilution.
- Applies comment weights based on likes and text depth.
- Scores only comments whose `comment_id` has not already been processed unless forced.
- Rebuilds `vehicle_sentiment_index` from the persistent `youtube_comments_scored` table after each run.

Sentiment features are intended to support the capstone question about whether consumer perception improves price or depreciation models.

### Exploratory Analysis

`EDA/EDA_notebook.ipynb` is the main Python EDA notebook. It focuses on data quality, schema overview, deterministic samples, price distributions, VIN duplication, feature engineering recommendations, and optional full-scan checks.

`EDA/Depreciation_Analysis.py` provides targeted depreciation exploration for selected makes, models, and model years. It compares early historical prices to current listing prices and generates Plotly visualizations.

`EDA/EDA_r.R` mirrors major exploratory views in R with DBI, dplyr, ggplot2, and RSQLite.

### Machine Learning

`ML/Price_ML_Models.py` trains current-price prediction models from `CAR_DATA_CLEANED.db`.

Important design choices:

- Bounded SQLite reads by default to keep development runs feasible.
- Full-database runs are opt-in with `--sample-size 0`; hyperparameter search
  uses a representative 200k-row tuning sample, then refits the tuned model on
  the full training split.
- Feature engineering for age, mileage, recency, ZIP region, listing text lengths, title keywords, EV/hybrid status, body/fuel segments, and make/model/year combinations.
- Latest-row-per-VIN deduplication by default.
- Time cutoff validation when possible, with VIN overlap removed from train rows.
- Group shuffle fallback by VIN.
- Target-derived `price_band` is used only for diagnostics and excluded from model inputs.
- Candidate models include Ridge, ElasticNet, LightGBM, Random Forest, and a segmented high-value LightGBM router.
- Outputs include JSON and Markdown reports plus `.joblib` model artifacts.

`ML/Time_Series_Price.py` trains cohort-level depreciation forecasts.

Important design choices:

- Cohort grain is make, model, model year, and trim proxy.
- Weekly cohort frames are built from price history.
- Features include market index, cohort lags, rolling prices, mileage, volume, NHTSA attributes, recall/complaint counts, and optional sentiment signals.
- Models forecast future depreciation percentages for configurable horizons such as 30, 90, 180, and 365 days.
- The script uses global models across cohorts to share signal across sparse vehicle segments.
- Horizon-specific hyperparameters are tuned on a representative bounded cohort-week sample with an inner temporal holdout, then refit on the full training frame.

`ML/Model_Output.ipynb` reads generated reports and presents a KPI-style model summary.

## Validation and Testing

Recommended validation commands:

```powershell
python Utilities\health_check.py
python Utilities\verify_schema.py
python -m unittest tests\test_ml_upgrade.py
python -m py_compile DataPipeline\Playwright_test.py DataPipeline\DataAquisition.py DataPipeline\DataCleaning.py DataPipeline\NHTSA_enrichment.py DataPipeline\SentimentAnalysis.py DataPipeline\absa_pipeline.py ML\Price_ML_Models.py ML\Time_Series_Price.py Utilities\health_check.py
```

`tests/test_ml_upgrade.py` currently checks:

- Cleaned output preserves key predictive listing fields and indexes.
- Current-price train/test splitting has no VIN overlap.
- Price-history gap loading correctly labels duplicate-like trajectories.

`tests/test_sentiment_incremental.py` checks:

- Video queue prioritization for unseen, stale, and partially completed playlist entries.
- Zero-comment and quota-exhausted resume behavior.
- Incremental ABSA loading by `comment_id`.
- Scored-comment upserts and aggregate rebuild behavior.

## Operational Runbook

Health check:

```powershell
python Utilities\health_check.py
```

Core pipeline:

```powershell
python DataPipeline\Playwright_test.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py
```

Windows scheduled pipeline:

```powershell
run_pipeline_scheduler.bat --dry-run
run_pipeline_scheduler.bat
```

Sentiment ingestion:

```powershell
python DataPipeline\SentimentAnalysis.py --playlist-id PLAYLIST_ID --max-videos 10 --max-comments 100
python DataPipeline\SentimentAnalysis.py --refresh-days 30 --force-recheck
python DataPipeline\absa_pipeline.py --run-all --limit 1000
python DataPipeline\absa_pipeline.py --run-all --force-reprocess
```

Current-price modeling:

```powershell
python ML\Price_ML_Models.py
```

Current-price plus depreciation modeling:

```powershell
python ML\Price_ML_Models.py --task all
```

Depreciation forecasting only:

```powershell
python ML\Time_Series_Price.py
```

## Known Caveats

- `DataPipeline/Playwright_test.py` is the current scraper even though the filename still reads like a test script.
- `DataPipeline/DataAquisition.py` keeps the historical misspelling in its filename and is now the Selenium reference/fallback path. Do not rename it casually because scripts and historical docs may reference it.
- Playwright browser installation may require a separate setup step depending on the environment.
- Some utility and EDA scripts still contain absolute Windows paths. Prefer repo-root-relative paths when touching them.
- `Utilities/fix_database_schema.py` is additive and backs up the raw database before migration, but agents should avoid running it unless schema verification shows it is needed.
- YouTube ingestion requires `YOUTUBE_API_KEY` or `GOOGLE_API_KEY`.
- Large database scans can be expensive. Use sample-size defaults unless a full capstone run is intentional.
- When adding research claims or new techniques, verify against recent primary sources, official docs, or peer-reviewed work and record the rationale in model reports or project docs.
