# Automotive Market ML Capstone

This repository is a master's capstone project for studying the current new and used car market with web-scraped listing data, official NHTSA enrichment, consumer sentiment, and machine learning. The project focuses on price prediction, depreciation forecasting, and the measurable value of safety technology, vehicle attributes, and public sentiment in automotive pricing.

The pipeline captures vehicle listings from AutoTempest-style result pages, stores longitudinal snapshots in SQLite, enriches VINs with NHTSA specifications, safety, recall, and complaint data, and trains models for current-price prediction and cohort-level depreciation forecasting.

## Research Goals

1. Quantify how safety technology, safety ratings, recalls, and complaints relate to resale price and depreciation.
2. Improve current vehicle price prediction with enriched NHTSA attributes, listing metadata, mileage, geography, and market timing.
3. Model depreciation as a time-series problem using price history by make, model, model year, and trim-like cohorts.
4. Evaluate whether YouTube comment sentiment and aspect-based sentiment analysis add predictive signal beyond structured vehicle data.
5. Keep the analysis reproducible enough for academic review: bounded data loads, leakage-safe validation, documented assumptions, and clear model reports.

## Repository Layout

```text
Car-Price-Data-Visualization-Learning/
|-- AGENTS.md                         # Root instructions for coding agents
|-- README.md                         # Project overview and runbook
|-- PROJECT_SUMMARY.md                # Technical architecture and capstone narrative
|-- requirements.txt                  # Python dependencies
|-- streamlit_app.py                  # Interactive dashboard, model scoring, and vehicle estimator
|-- run_pipeline_scheduler.bat        # Windows scheduler entry point
|-- run_pipeline_scheduler.sh         # macOS/Linux scheduler entry point
|-- DataPipeline/
|   |-- Playwright_test.py            # Current Playwright global-queue scraper
|   |-- DataAquisition.py             # Legacy/reference Selenium CDP scraper
|   |-- database.py                   # Listing schema plus separate NHTSA store
|   |-- NHTSA_enrichment.py           # Complete vPIC, ratings, recalls, complaints
|   |-- DataCleaning.py               # Polars-based cleaned database builder
|   |-- VehicleNormalization.py        # EPA cache and canonical identity parser
|   |-- SentimentAnalysis.py          # YouTube comments ingestion
|   |-- absa_pipeline.py              # Aspect-based sentiment scoring
|   |-- migrate_to_db.py              # Legacy CSV to SQLite migration helper
|-- EDA/
|   |-- EDA_notebook.ipynb            # Data quality and exploratory analysis
|   |-- Depreciation_Analysis.py      # Targeted depreciation exploration
|   |-- EDA_r.R                       # R/ggplot2 exploratory analysis
|-- ML/
|   |-- Price_ML_Models.py            # Current-price modeling pipeline
|   |-- Time_Series_Price.py          # Cohort depreciation forecasting
|   |-- Model_Output.ipynb            # KPI-style model output notebook
|-- Utilities/
|   |-- health_check.py               # Dependency, config, file, and schema checks
|   |-- verify_schema.py              # Raw database schema inspection
|   |-- fix_database_schema.py        # Additive schema migration helper
|-- tests/
|   |-- test_ml_upgrade.py            # Cleaning and modeling regression tests
|-- CAR_DATA_OUTPUT/                  # SQLite databases, logs, exports
|-- MODELS_OUTPUT/                    # Model artifacts and generated reports
```

## Quick Start

Create or activate a Python environment, then install dependencies:

```powershell
.\\.venv\\Scripts\\python.exe -m pip install -r requirements.txt
python -m playwright install
python Utilities\health_check.py
```

Both scheduler scripts prefer the repo-local `.venv` when it exists. The
macOS/Linux script also recognizes the repository's legacy `venv` directory
before falling back to `python3` or `python` on `PATH`.

On macOS, create the environment and install Playwright with:

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
./.venv/bin/python -m playwright install
```

Run the core data pipeline:

```powershell
python DataPipeline\Playwright_test.py
python DataPipeline\NHTSA_enrichment.py --resume --refresh-days 30
python DataPipeline\DataCleaning.py
```

The first historical refresh should be run deliberately with
`--refresh-all`. Use a positive `--max-vins` value for a bounded smoke run:

```powershell
python DataPipeline\NHTSA_enrichment.py --refresh-all --max-vins 50 --rate-limit-delay 1.0
python Utilities\verify_schema.py --nhtsa-db-path CAR_DATA_OUTPUT\CAR_DATA_NHTSA.db
```

Before the first run against an existing CAR_DATA.db, create a one-time
recoverable backup with --backup-path
CAR_DATA_OUTPUT\backups\CAR_DATA_pre_nhtsa.db. The command refuses to overwrite
an existing backup; routine scheduled runs do not repeat this multi-gigabyte
operation.

NHTSA enrichment resumes from successful cached responses by default. The
pipeline stores normalized response history in `CAR_DATA_NHTSA.db`; the legacy
`nhtsa_enrichment` table remains a latest-value compatibility projection. Schema
version 2 intentionally omits full API-response JSON, per-record JSON copies,
request payloads, and raw bulk-row blobs. Recreate a schema-version-1 NHTSA
database before running this version; the script refuses to mix both layouts.
Use --backfill-legacy when you want a self-documenting full historical refresh; it is an alias for --refresh-all and bypasses the response freshness cache.

Run the complete macOS pipeline in sequence:

```bash
./run_pipeline_scheduler.sh --dry-run
./run_pipeline_scheduler.sh
```

Each command waits for the previous Python process to finish. The pipeline stops
immediately and returns that process's exit code if any stage fails.

Cleaning refreshes the official FuelEconomy.gov `vehicles.csv.zip` catalog into
`CAR_DATA_OUTPUT/reference/epa_fuel_economy/` and imports it into the cleaned
database. Use `--offline` or `--no-epa-refresh` when a validated cache already
exists. NHTSA make/model are the canonical anchors and title agreement is
audited; trim text is derived only from the listing title. EPA validates and
standardizes that title-derived result. Raw `nhtsa_Trim` and `nhtsa_Trim2`
remain untouched comparison fields.

Refresh or re-import only the EPA reference tables without rebuilding listings:

```powershell
python DataPipeline\VehicleNormalization.py --no-refresh --import-db CAR_DATA_OUTPUT\CAR_DATA_CLEANED.db
```

Train current-price models:

```powershell
python ML\Price_ML_Models.py
```

The default run uses every eligible VIN. SQLite selects the latest valid
listing snapshot per VIN before joining enrichment data, then pandas reads the
result in bounded chunks with Arrow-backed strings and nullable 32-bit numeric
columns. This avoids materializing every historical snapshot as one large
`float64` block.

For a bounded smoke run, use `--sample-size 5000`. A positive sample size scopes
listing snapshots before latest-per-VIN selection; `--sample-size 0` explicitly
requests the same full eligible-VIN behavior as the default. Hyperparameter
search still uses a representative 200k-row tuning sample, then refits the tuned
model on the full training split where the model family supports it.

Train current-price and depreciation models together:

```powershell
python ML\Price_ML_Models.py --task all
```

TimesFM downloads its configured checkpoint from Hugging Face. To authenticate
those requests, create a repository-root `.env` file and add your token without
quotes:

```dotenv
HF_TOKEN=hf_replace_with_your_token
```

`ML/Time_Series_Price.py` loads this file before importing TimesFM. A token
already set in the shell environment takes precedence, and `.env` is ignored by
Git so credentials are not committed.

Because the current-price default sample size is `0`, `--task all` also requests
full price-history input from the depreciation workflow and can take several
hours. Add `--sample-size 5000` for a bounded end-to-end smoke run.

Launch the Streamlit dashboard:

```powershell
streamlit run streamlit_app.py
```

Run regression tests:

```powershell
python -m unittest tests\test_ml_upgrade.py
python -m unittest tests\test_vehicle_normalization.py tests\test_streamlit_canonical.py
```

## Data Workflow

1. `DataPipeline/Playwright_test.py` is the active scraper. It runs a global queue of `(make, source button)` tasks, intercepts `queue-results` network responses, and writes results incrementally to SQLite.
2. `DataPipeline/database.py` stores listing snapshots, normalized `price_history`, normalized `listing_history`, and the compatibility NHTSA snapshot in `CAR_DATA.db`; normalized NHTSA history is stored in `CAR_DATA_NHTSA.db` without raw/full-response JSON duplication.
3. `DataPipeline/NHTSA_enrichment.py` refreshes all historical VINs incrementally with complete vPIC fields, two-step safety ratings, full recall/complaint records, request metadata, and listing fallback identity context.
4. `DataPipeline/DataCleaning.py` builds `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db`, anchors canonical make/model to NHTSA, derives canonical trim only from listing titles, validates against the cached EPA catalog, creates VIN consensus identities, and adds audit indexes.
5. `DataPipeline/SentimentAnalysis.py` discovers playlist videos, resumes comment collection at the video level, and stores progress in `CAR_YOUTUBE_COMMENTS.db`.
6. `DataPipeline/absa_pipeline.py` scores only new attributable comments by `comment_id` and rebuilds current plus monthly make-level sentiment indexes from stored scored rows.
7. `ML/Price_ML_Models.py` trains leakage-aware current-price models and writes reports to `MODELS_OUTPUT/`.
8. `ML/Time_Series_Price.py` trains cohort-level monthly depreciation forecasts from historical price trajectories with global ML, SARIMAX, Prophet, and TimesFM model families when their dependencies are installed.
9. `streamlit_app.py` reads `CAR_DATA_CLEANED.db` and `MODELS_OUTPUT/` artifacts to present filterable VIN actuals, NHTSA KPIs, global and filter-scoped model metrics, current-price predictions, cohort future-price forecasts, time-series backtesting KPIs, and a custom Vehicle estimator.

The Vehicle estimator accepts canonical make, model, year, trim, and mileage inputs,
with optional transmission and engine profiles selected from the same-year cohort.
It fills remaining current-price fields from deterministic modal values and shows
their provenance before scoring every available current-price artifact. Its
depreciation view presents both a predicted-price anchor and an observed-cohort-
median anchor using the global monthly model. Saved SARIMAX, Prophet, and TimesFM
paths are shown separately as cohort references; they do not apply the custom
vehicle's mileage or technical overrides.

Current-price artifacts are trained by direct script execution, so the dashboard
registers their custom estimator and transformer symbols before joblib loading.
If an artifact cannot be loaded or scored, the Model Predictions tab preserves
the metric-table schema and reports the model-specific error instead of failing
the entire page.

## Modeling Approach

The current-price pipeline uses `canonical_make`, `canonical_model`, `canonical_year`, and `canonical_trim`, plus engineered mileage/age, market, safety, and listing features. Raw NHTSA trims, legacy combined trims, identity provenance, confidence, agreement flags, and EPA IDs are excluded from predictive inputs. Candidate models are Ridge, ElasticNet, RandomForest, and LightGBM; each candidate first trains a leakage-safe classifier to route everyday versus high-value vehicles before fitting separate segment regressors.

The depreciation pipeline builds monthly cohorts by make, model, model year, and cleaned trim proxy. It trains a global one-month depreciation model across related vehicle cohorts, then recursively emits a month-by-month median-price forecast for the next 60 months by default. It also benchmarks local SARIMAX, Prophet, and TimesFM cohort forecasts when dependencies and sufficient cohort history are available. Forecast origins use each cohort's latest retained price-history month, and the default run does not impose a global upper-price cap so high-value cohorts keep their full observed history. Sparse and newer cohorts borrow signal from related make/model/year/trim cohorts through the global model; local model families are capped by default for bounded iteration and can be run across all eligible cohorts with `--max-local-model-cohorts 0`.

## Sentiment and NLP

`SentimentAnalysis.py` uses the YouTube Data API to collect comments for configured videos or playlists, but now keeps a per-video resume queue in SQLite so unseen videos run first, retryable errors back off, and completed videos refresh on a 30-day window by default. `absa_pipeline.py` attributes comments to one unambiguous canonical make, filters low-quality comments, runs zero-shot aspect classification on new attributable comments only, and rebuilds current plus cumulative monthly make-level sentiment tables. Ambiguous multi-make comments are retained for audit but excluded from aggregates.

Useful sentiment commands:

```powershell
python DataPipeline\SentimentAnalysis.py --playlist-id PLAYLIST_ID --max-videos 10 --max-comments 100
python DataPipeline\SentimentAnalysis.py --refresh-days 30 --force-recheck
python DataPipeline\absa_pipeline.py --migrate-make-grain
python DataPipeline\absa_pipeline.py --run-all --limit 1000
python DataPipeline\absa_pipeline.py --run-all --force-reprocess
```

Use `--migrate-make-grain` once to backfill existing scored comments without rerunning the Hugging Face model. Set `YOUTUBE_API_KEY` or `GOOGLE_API_KEY` in the environment or `.env` before ingestion. Public Hugging Face models do not require authentication; optional Hub authentication uses `HF_TOKEN` from the same ignored `.env` file.

## Databases and Outputs

- `CAR_DATA_OUTPUT/CAR_DATA.db`: raw listing snapshots, history tables, and the backward-compatible latest NHTSA projection.
- `CAR_DATA_OUTPUT/CAR_DATA_NHTSA.db`: one dynamically widened vPIC value row per decode, identity resolutions, safety variants/fields, recall and complaint/product records, ingestion runs, API extra fields, and normalized bulk-source fields. Full response and raw-row JSON blobs are not stored.
- `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db`: cleaned analysis/modeling database, including `vehicle_identity`, `epa_vehicle_catalog`, and `epa_catalog_metadata`.
- `CAR_DATA_OUTPUT/reference/epa_fuel_economy/`: ignored local cache of the official [FuelEconomy.gov vehicle dataset](https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip).
- `CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db`: YouTube comment and sentiment-derived tables, including `youtube_video_fetch_state`, `youtube_playlist_fetch_state`, `youtube_comments_scored`, `make_sentiment_index`, and `make_sentiment_monthly`.
- `MODELS_OUTPUT/model_report.json` and `MODELS_OUTPUT/model_report.md`: current-price model reports.
- `MODELS_OUTPUT/cohort_depreciation_model_report.json` and `.md`: depreciation model reports.
- `MODELS_OUTPUT/cohort_future_forecasts.csv`: monthly future cohort median-price forecasts by model family used by the dashboard.
- `MODELS_OUTPUT/cohort_backtesting_results.csv`: leakage-safe expanding rolling-origin cohort/model/horizon predictions, actuals, and origin-window metadata. Each origin requires at least six prior cohort months.
- `MODELS_OUTPUT/cohort_backtesting_kpis.csv`: aggregate and canonical cohort-level rolling backtesting KPIs for depreciation forecasts.
- `MODELS_OUTPUT/*.joblib`: trained model artifacts.

## Notes for Contributors and Agents

- Read `AGENTS.md` before making changes.
- Preserve API interception as the acquisition strategy. Do not replace structured network capture with HTML scraping.
- Treat `DataPipeline/Playwright_test.py` as the current scraper unless the user explicitly asks to work on the Selenium fallback.
- Keep NHTSA-derived columns prefixed with `nhtsa_`.
- Use Polars for new cleaning work unless a touched area already requires Pandas.
- Avoid target leakage in modeling. Do not train on fields derived from the target price.
- When adding dependencies, update `requirements.txt` in the same change.
- When making research claims or recommending new techniques, verify against current primary sources or official documentation.
- NHTSA recall and complaint records are generally make/model/year-level evidence, not VIN-specific reliability rates. Preserve source grain and report exposure limitations before interpreting them as reliability measures.
- Any change to NHTSA endpoints, schemas, refresh behavior, scheduler commands, or data lineage must update the README, PROJECT_SUMMARY, AGENTS.md, and relevant `.github` guidance in the same change.
