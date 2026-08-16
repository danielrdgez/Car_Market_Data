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

Dashboard
  CAR_DATA_CLEANED.db and MODELS_OUTPUT artifacts
  -> Streamlit VIN actuals, model metrics, predictions, forecasts
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
- Applies contextual price-outlier removal with robust medians and quantile fences by canonical make, model, model year, and title-derived trim when enough support exists; repeated-digit prices such as `444444` are dropped only when they are extreme relative to the relevant cohort.
- Treats `nhtsa_Make` and `nhtsa_Model` as canonical anchors when present and records whether the listing title corroborates year, make, and model.
- Derives `canonical_trim` only from the listing title. The official FuelEconomy.gov catalog standardizes or validates that title-derived result; an unmatched remainder is retained and an empty remainder becomes the explicit `UNKNOWN_TRIM` failure state.
- Preserves `nhtsa_Trim` and `nhtsa_Trim2` unchanged as diagnostic comparison fields. They cannot supply or override canonical trim text.
- Retains legacy `title_trim`, `trim_combined`, and `trim_source` as canonical-backed compatibility fields while downstream ML uses `canonical_trim`.
- Imports the complete cached EPA file into `epa_vehicle_catalog`, records provenance in `epa_catalog_metadata`, and creates a confidence/recency-ranked VIN consensus in `vehicle_identity`.
- Fills missing or non-positive `nhtsa_BasePrice` from the earliest cleaned `price_history` price for the VIN, then the earliest cleaned `listing_history` price when price history is unavailable, while recording `nhtsa_BasePrice_source`.
- Normalizes date and numeric columns.
- Retains every valid NHTSA-enriched make/model/model-year row; there is no hard-coded make whitelist.
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

- Full eligible-VIN training is the default. SQLite filters invalid target rows
  and selects the latest listing snapshot per VIN before joining NHTSA data, so
  repeated snapshots are not materialized only to be discarded later.
- Query results are read in bounded chunks with Arrow-backed pandas columns;
  SQLite integers and reals are narrowed to nullable 32-bit types before chunks
  are combined.
- Positive `--sample-size` values remain available for bounded development.
  Hyperparameter search uses a representative 200k-row tuning sample, then
  refits the tuned model on the full training split.
- Canonical identity features come exclusively from `canonical_make`, `canonical_model`, `canonical_year`, and title-derived `canonical_trim`; raw/legacy trim candidates and identity diagnostics are excluded from the feature matrix.
- Feature engineering for age, mileage, recency, ZIP region, listing text lengths, title keywords, EV/hybrid status, body/fuel segments, and canonical make/model/year/trim combinations.
- Latest-row-per-VIN deduplication by default.
- Time cutoff validation when possible, with VIN overlap removed from train rows.
- Group shuffle fallback by VIN.
- Target-derived `price_band` is excluded from model inputs; it is used for diagnostics and for training the high-value classifier labels inside the training split.
- Candidate models include Ridge, ElasticNet, LightGBM, and RandomForest, and every candidate uses the same leakage-safe everyday/high-value classifier router before fitting segment-specific regressors.
- Outputs include JSON and Markdown reports plus `.joblib` model artifacts.

`ML/Time_Series_Price.py` trains cohort-level depreciation forecasts.

Important design choices:

- Cohort grain is canonical make, model, model year, and trim; VIN assignment comes from `vehicle_identity` for stability across snapshots.
- Monthly cohort frames are built from price history.
- Features include market index, cohort lags, rolling prices, mileage, volume, NHTSA attributes, recall/complaint counts, and optional sentiment signals.
- Models forecast one-month depreciation percentages and recursively emit a monthly median-price path up to five years ahead by default.
- The time-series benchmark now includes global ML, SARIMAX, Prophet, and TimesFM model families when optional dependencies are installed and cohorts have enough monthly support.
- The time-series entry point loads `HF_TOKEN` from the repository-root `.env` before importing TimesFM; an existing shell environment value takes precedence and the local file is Git-ignored.
- Backtesting outputs are written as row-level cohort/model/horizon results plus KPI tables with future-price MAE, WAPE, bias, depreciation error, R2, and skill against a no-change baseline.
- Forecast origins use each cohort's latest retained price-history month; normal runs keep high-value histories because `--max-price` defaults to disabled and is only an opt-in sensitivity cap.
- The script uses global models across cohorts to share signal across sparse vehicle segments.
- Target-specific hyperparameters are tuned on a representative bounded cohort-month sample with an inner temporal holdout, then refit on the full training frame.

#### Machine Learning Pipeline Diagram

The two modeling entry points share the cleaned vehicle database but use different
observational grains, validation strategies, encoders, and forecast outputs.

```mermaid
flowchart TB
    DB[(CAR_DATA_CLEANED.db)]
    ABSA[(Optional CAR_YOUTUBE_COMMENTS.db<br/>Vehicle_Sentiment_Index)]

    subgraph CP[Current-price pipeline - Price_ML_Models.py]
        CP_LOAD[Latest eligible listing snapshot per VIN<br/>Inner join NHTSA enrichment<br/>Optional ABSA vehicle sentiment join<br/>Chunked Arrow-backed SQLite reads]
        CP_FILTER[Validate positive price and nonnegative mileage<br/>Require canonical make, model, year, and title-derived trim<br/>Deduplicate to one current row per VIN]
        CP_FE[Engineer current-price features<br/>Age, mileage, recency, location, text, keywords,<br/>canonical identity combinations, market and safety signals]
        CP_LEAK[Leakage guard<br/>Remove price, price_band, VIN/date metadata,<br/>NHTSA base price, legacy identity, and audit fields]
        CP_SPLIT{Validation split}
        CP_TIME[Preferred: time cutoff<br/>Remove training VINs appearing in test]
        CP_GROUP[Fallback: GroupShuffleSplit by VIN]
        CP_NUM[Numeric branch<br/>Median imputation and float32 cast]
        CP_LOW[Low-cardinality categorical branch<br/>UNKNOWN imputation and infrequent-aware one-hot encoding]
        CP_HIGH[High-cardinality or identity branch<br/>UNKNOWN imputation and smoothed target encoding]
        CP_COMBINE[ColumnTransformer<br/>Combine numeric, one-hot, and target-encoded blocks]
        CP_SCALE[Linear candidates only<br/>Sparse-safe StandardScaler]
        CP_MODELS[Ridge | ElasticNet | LightGBM | RandomForest<br/>All regressors use log1p target transformation]
        CP_TUNE[Randomized hyperparameter search<br/>Up to 200k stratified rows and GroupKFold by VIN<br/>Refit selected settings on the training split]
        CP_ROUTE[Leakage-safe high-value router<br/>Training-label classifier for price above 150k<br/>Everyday and high-value experts plus global blend]
        CP_EVAL[Test evaluation<br/>MAE, RMSE, RMSLE, MAPE, R2<br/>Price-band, high-value, make, and year segments]
        CP_SELECT[Select lowest-MAE candidate]
        CP_OUT[MODELS_OUTPUT<br/>Candidate and best-model joblib artifacts<br/>JSON and Markdown reports<br/>Feature-weight CSV]

        CP_LOAD --> CP_FILTER --> CP_FE --> CP_LEAK --> CP_SPLIT
        CP_SPLIT --> CP_TIME
        CP_SPLIT --> CP_GROUP
        CP_TIME --> CP_NUM
        CP_TIME --> CP_LOW
        CP_TIME --> CP_HIGH
        CP_GROUP --> CP_NUM
        CP_GROUP --> CP_LOW
        CP_GROUP --> CP_HIGH
        CP_NUM --> CP_COMBINE
        CP_LOW --> CP_COMBINE
        CP_HIGH --> CP_COMBINE
        CP_COMBINE -->|Tree candidates| CP_MODELS
        CP_COMBINE --> CP_SCALE -->|Linear candidates| CP_MODELS
        CP_MODELS --> CP_TUNE --> CP_ROUTE --> CP_EVAL --> CP_SELECT --> CP_OUT
    end

    subgraph TS[Cohort-depreciation pipeline - Time_Series_Price.py]
        TS_LOAD[Price history plus latest listing identity<br/>VIN consensus identity preferred<br/>NHTSA attributes and optional vehicle sentiment]
        TS_CLEAN[Clean positive dated observations<br/>Optional max-price sensitivity cap<br/>Normalize canonical make, model, year, and trim proxy]
        TS_COHORT[Monthly make-model-year-trim cohorts<br/>Require configured VIN and history support]
        TS_FE[Aggregate and engineer cohort-time features<br/>Price, mileage, volume, calendar, market index,<br/>lags, rolling windows, safety, and sentiment]
        TS_TARGET[Origin-safe targets<br/>Future median price and depreciation by horizon<br/>Future target columns never enter model features]
        TS_SUPPORT{At least 50 complete<br/>cohort-month rows?}
        TS_SKIP[Skip horizon and record reason]
        TS_NUM[Numeric branch<br/>Median imputation]
        TS_CAT[Categorical branch<br/>UNKNOWN imputation and unknown-safe ordinal encoding]
        TS_GUARD{At least two training rows<br/>and a varying target?}
        TS_CONST[DummyRegressor mean baseline]
        TS_GLOBAL[Global supervised model<br/>LightGBM when installed<br/>Otherwise HistGradientBoosting]
        TS_TUNE[Bounded stratified tuning sample<br/>Inner temporal holdout<br/>Refit on full horizon training frame]
        TS_BACKTEST[Expanding rolling-origin backtest<br/>Each origin uses only targets observable by that date]
        TS_RECURSE[Recursive global monthly path<br/>Default 60-month forecast]
        TS_LOCAL[Eligible local cohort histories]
        TS_SARIMAX[SARIMAX]
        TS_PROPHET[Prophet]
        TS_TIMESFM[TimesFM]
        TS_LOCAL_BT[Local holdout backtests and future paths]
        TS_OUT[MODELS_OUTPUT<br/>Horizon joblib artifacts and model reports<br/>Future forecasts, row-level backtests, and KPI CSVs]

        TS_LOAD --> TS_CLEAN --> TS_COHORT --> TS_FE --> TS_TARGET --> TS_SUPPORT
        TS_SUPPORT -->|No| TS_SKIP --> TS_OUT
        TS_SUPPORT -->|Yes| TS_NUM --> TS_GUARD
        TS_SUPPORT -->|Yes| TS_CAT --> TS_GUARD
        TS_GUARD -->|No| TS_CONST --> TS_BACKTEST
        TS_GUARD -->|Yes| TS_TUNE --> TS_GLOBAL --> TS_BACKTEST --> TS_RECURSE --> TS_OUT
        TS_COHORT --> TS_LOCAL
        TS_LOCAL --> TS_SARIMAX --> TS_LOCAL_BT
        TS_LOCAL --> TS_PROPHET --> TS_LOCAL_BT
        TS_LOCAL --> TS_TIMESFM --> TS_LOCAL_BT
        TS_LOCAL_BT --> TS_OUT
    end

    DB --> CP_LOAD
    DB --> TS_LOAD
    ABSA -. optional .-> CP_LOAD
```

##### Current-price feature and encoder reference

The exact raw columns are schema-driven: eligible cleaned listing fields, NHTSA
enrichment fields other than base-price leakage fields, and the optional ABSA
aggregates are retained unless they appear in the exclusion policy below. The
generated `model_report.json` records the resolved numeric and categorical lists
for each run.

| Feature or processing group | Inputs and derived fields | Encoder or transformation |
|---|---|---|
| Canonical identity | `canonical_make`, `canonical_model`, `canonical_year`, `canonical_trim`; derived `trim_proxy`, `make_model_year`, and `make_model_year_trim` | Identity-like categorical columns are forced into the high-cardinality target-encoding branch. Only title-derived canonical trim can become a trim predictor. |
| Mileage and age | `mileage`; derived `vehicle_age`, `vehicle_age_squared`, `miles_per_year`, `log_mileage`, `mileage_age_interaction`, `mileage_bucket`, and `model_year_bucket` | Numeric values use median imputation and float32 conversion. Buckets are encoded according to their observed cardinality. |
| Listing time and geography | `loaddate`; derived `listing_recency_days`, `listing_month`, `listing_week`, and two-digit `location_region` from `locationCode` | Numeric calendar fields use median imputation. Location-like categoricals are target encoded. |
| Listing text and state | Available title fields; derived length/word counts, certified/CPO, AWD/4WD, luxury-trim mentions, `pendingSale`, `priceRecentChange`, and `source_is_marketplace` | Counts and flags use the numeric branch. Raw title metadata is excluded, while derived text features remain eligible. |
| Vehicle, market, safety, and sentiment | Cleaned listing attributes, usable NHTSA attributes, `body_fuel_segment`, `is_ev_or_hybrid`, and optional `Reliability_Index`, `General_Enthusiast_Score`, `Sentiment_Volatility_StdDev`, `Sentiment_Trend_Slope`, and `Confidence_Level` | Numeric fields use median imputation; categoricals with at most 50 values use one-hot unless identity-like, while the remainder use target encoding. |
| Low-cardinality categoricals | Non-identity categorical columns with at most 50 observed values | Normalize missing values, impute `UNKNOWN`, then `OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=10, max_categories=25)` with sparse float32 output. |
| High-cardinality categoricals | Columns over 50 values plus names containing make/model/trim/manufacturer/segment/title/location tokens | Normalize missing values, impute `UNKNOWN`, then `TargetEncoder(min_samples_leaf=20, smoothing=10)` and float32 conversion. |
| Linear candidate preprocessing | Combined numeric, one-hot, and target-encoded matrix for Ridge and ElasticNet | `StandardScaler(with_mean=False)` preserves sparse compatibility. Tree candidates use the unscaled combined matrix. |
| Excluded leakage and metadata | `price`, `price_band`, `nhtsa_BasePrice`, `nhtsa_BasePrice_source`, VIN/date/title identifiers, legacy/raw trim fields, canonical audit/provenance fields, EPA IDs, and identity agreement flags | Dropped before preprocessing. `price_band` remains diagnostic-only, and the high-value label is created only inside training. |

##### Cohort-depreciation feature and encoder reference

| Feature or processing group | Exact model inputs | Encoder or transformation |
|---|---|---|
| Cohort identity | `make`, `model`, `model_year`, `trim_proxy` | Constant `UNKNOWN` imputation followed by `OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1)`. |
| Vehicle categories | `body_class`, `drive_type`, `fuel_type`, `electrification_level`, `dominant_seller_type`, `dominant_source_name` | Same unknown-safe ordinal encoding as cohort identity. |
| Price and mileage state | `median_price`, `avg_price`, `price_p25`, `price_p75`, `avg_mileage`, `median_mileage`, `avg_vehicle_age_months`, `avg_miles_per_year` | Median imputation. These values are observed at the forecast origin, not future targets. |
| Volume and calendar | `volume`, `unique_vins`, `price_down_rate`, `month`, `quarter`, `cohort_month_number`, `cohort_age_months` | Median imputation. |
| Cohort trajectory | `cohort_first_median_price`, `price_index_vs_cohort_first`, `cumulative_depreciation_pct`, `lag_median_price_1`, `lag_median_price_2`, `lag_price_index_1`, `rolling_median_price_3m`, `rolling_avg_mileage_3m`, `rolling_volume_3m`, `rolling_depreciation_pct_3m` | Median imputation; lag and rolling values are constructed from observations available at the forecast origin. |
| Market context | `market_median_price`, `market_price_index`, `market_monthly_volume` | Median imputation. |
| Sentiment, powertrain, and safety | `sentiment_score`, `sentiment_comment_count`, `sentiment_video_count`, `engine_hp`, `engine_cylinders`, `total_recalls`, `total_complaints` | Median imputation; sentiment remains optional when source data is unavailable. |
| Targets and excluded leakage | `target_depreciation_pct_{horizon}m` is the supervised target; `target_median_price_{horizon}m` is retained for evaluation. `nhtsa_BasePrice`, `nhtsa_BasePrice_source`, and all future target columns are excluded from predictors. | Target rows are aligned by future cohort month. Rolling-origin validation restricts training to targets observable by each origin. |

`ML/Model_Output.ipynb` reads generated reports and presents a KPI-style model summary.

### Streamlit Dashboard

`streamlit_app.py` provides an interactive UI over the cleaned database and generated model artifacts. Filters and primary labels use canonical identity; raw titles and NHTSA trims remain visible for comparison. The app reports normalization coverage, EPA matching, unresolved titles, and NHTSA identity disagreement, warns on a missing canonical schema, and disables predictions when database and model normalization versions differ. Current-price joblib artifacts retain compatibility with direct-script training through an explicit custom-object registration layer. Filter-scoped scoring also preserves its metric schema when individual models fail, allowing the dashboard to show model-specific diagnostics without masking them behind a secondary table error.

## Validation and Testing

Recommended validation commands:

```powershell
python Utilities\health_check.py
python Utilities\verify_schema.py
python -m unittest tests\test_ml_upgrade.py
python -m unittest tests\test_vehicle_normalization.py
python -m py_compile DataPipeline\Playwright_test.py DataPipeline\DataAquisition.py DataPipeline\DataCleaning.py DataPipeline\VehicleNormalization.py DataPipeline\NHTSA_enrichment.py DataPipeline\SentimentAnalysis.py DataPipeline\absa_pipeline.py ML\Price_ML_Models.py ML\Time_Series_Price.py Utilities\health_check.py Utilities\verify_schema.py
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

macOS/Linux scheduled pipeline:

```bash
./run_pipeline_scheduler.sh --dry-run
./run_pipeline_scheduler.sh
```

The scheduler entry points run the same four blocking stages in order:
Playwright scraping, NHTSA enrichment, EPA reference refresh and validation,
then cleaning with `--no-epa-refresh`. They stop on the first failed stage.

Sentiment ingestion:

```powershell
python DataPipeline\SentimentAnalysis.py --playlist-id PLAYLIST_ID --max-videos 10 --max-comments 100
python DataPipeline\SentimentAnalysis.py --refresh-days 30 --force-recheck
python DataPipeline\absa_pipeline.py --run-all --limit 1000
python DataPipeline\absa_pipeline.py --run-all --force-reprocess
```

Current-price modeling:

```powershell
python ML\Price_ML_Models.py --sample-size 5000
# Explicit full-data run (also the default when the flag is omitted):
python ML\Price_ML_Models.py --sample-size 0
```

`python ML\Price_ML_Models.py --task all` passes the current-price sample size
through to depreciation. With the default `0`, both workflows use their full
eligible data; pass a positive sample size for a bounded end-to-end run.

Current-price plus depreciation modeling:

```powershell
python ML\Price_ML_Models.py --task all
```

Depreciation forecasting only:

```powershell
python ML\Time_Series_Price.py
```

Streamlit dashboard:

```powershell
streamlit run streamlit_app.py
```

## Known Caveats

- `DataPipeline/Playwright_test.py` is the current scraper even though the filename still reads like a test script.
- `DataPipeline/DataAquisition.py` keeps the historical misspelling in its filename and is now the Selenium reference/fallback path. Do not rename it casually because scripts and historical docs may reference it.
- Playwright browser installation may require a separate setup step depending on the environment.
- Some utility and EDA scripts still contain absolute Windows paths. Prefer repo-root-relative paths when touching them.
- `Utilities/fix_database_schema.py` is additive and backs up the raw database before migration, but agents should avoid running it unless schema verification shows it is needed.
- YouTube ingestion requires `YOUTUBE_API_KEY` or `GOOGLE_API_KEY`.
- Full current-price runs can take hours even with bounded-memory loading. Use a
  positive `--sample-size` for development and smoke tests.
- When adding research claims or new techniques, verify against recent primary sources, official docs, or peer-reviewed work and record the rationale in model reports or project docs.
