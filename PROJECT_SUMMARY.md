# Project Summary

Last updated: September 15, 2026

The [data dictionary](DATA_DICTIONARY.md) documents the inspected schemas of `CAR_DATA.db`, `CAR_YOUTUBE_COMMENTS.db`, and `CAR_DATA_NHTSA.db`: 32 project tables and 682 columns, plus SQLite internal sequence tables. It distinguishes stored constraints from intended behavior, documents lineage and sentiment calculations, and records legacy foreign-key and temporal-availability limitations. The cleaned database and model outputs are outside that dictionary's scope.

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
  -> CAR_DATA_NHTSA.db normalized source history
  -> nhtsa_enrichment compatibility projection

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
- `CAR_DATA_NHTSA.db`: normalized NHTSA source history and source-grain records, separate from the listing database, with no full-response or raw-row JSON blobs.
- `youtube_comments_sentiment`: YouTube comment ingestion output.

Important behavior:

- Acquisition writes are incremental.
- Thread-safe acquisition uses thread-local SQLite connections and a shared write lock.
- History tables use `INSERT OR IGNORE` with uniqueness constraints.
- NHTSA records are deduplicated by individual result/record hashes and retained in typed tables, one dynamically widened vPIC row per decode, or normalized field/value tables for sparse extras; transport wrappers and duplicate JSON payloads are discarded. The primary database receives an explicit compatibility projection.

### NHTSA Enrichment

`DataPipeline/NHTSA_enrichment.py` refreshes every distinct VIN present in `listings`, using the separate `CAR_DATA_NHTSA.db` as the normalized NHTSA store and retaining `nhtsa_enrichment` as a latest-successful compatibility projection.

Sources:

- vPIC VIN decode fields.
- NHTSA SafetyRatings API.
- NHTSA Recalls API.
- NHTSA Complaints API.
- Official downloadable NHTSA investigation, complaint, recall, manufacturer communication, and technical-service-bulletin files can be imported through the bulk importer without dropping source fields; rows are flattened into `nhtsa_bulk_fields` rather than retained as opaque JSON.

The enricher batches VIN decode requests up to 50 VINs, includes listing model-year hints, uses conservative retries and rate limiting, caches successful responses by freshness window, performs the two-step Safety Ratings lookup for every returned `VehicleId`, and prefixes compatibility columns with `nhtsa_`. NHTSA make/model/year values are selected first; listing year and title-derived make/model are field-level fallbacks with source and conflict metadata.

Raw vPIC variables are stored dynamically as variable/value rows and raw JSON so newly added NHTSA fields are not silently discarded. Empty, invalid, missing-result, request-failed, and successful responses remain distinguishable. Recalls and complaints retain all response fields and are interpreted at their source grain rather than as direct VIN failure rates.

Useful commands:

```powershell
python DataPipeline\NHTSA_enrichment.py --refresh-all --max-vins 50 --rate-limit-delay 1.0
python DataPipeline\NHTSA_enrichment.py --resume --refresh-days 30
python Utilities\verify_schema.py --nhtsa-db-path CAR_DATA_OUTPUT\CAR_DATA_NHTSA.db
```

Use --backfill-legacy instead of --refresh-all when the purpose is explicitly to reprocess every historical VIN with the current NHTSA mappings and values.

For the first migration of an existing primary database, pass
--backup-path CAR_DATA_OUTPUT\backups\CAR_DATA_pre_nhtsa.db. The backup uses
SQLite's live-database backup API and refuses to overwrite an existing file;
scheduled incremental runs should omit this one-time option.

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
- Retains every listing even when NHTSA decoding is partial; NHTSA make/model/model-year remains preferred when present and listing identity supplies fallbacks with provenance.
- Does not copy the detailed NHTSA database into the cleaned database; cleaned output uses the compatibility projection while normalized source records remain queryable in `CAR_DATA_NHTSA.db`.
- Creates indexes for modeling and time-series reads.

The cleaned database is the preferred input for EDA and modeling.

### Sentiment and Aspect-Based NLP

`DataPipeline/SentimentAnalysis.py` uses the YouTube Data API to collect comments from configured videos or playlists. It now persists playlist discovery plus per-video fetch state in `CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db`, prioritizes unseen videos first, and refreshes completed videos on a bounded schedule instead of restarting every playlist from the top on each run.

`DataPipeline/absa_pipeline.py` performs aspect-based sentiment analysis:

- Attributes each comment to one canonical make, preferring a unique make named in the comment and otherwise using a unique make in the video title.
- Retains ambiguous or unknown make assignments for audit while excluding them from make aggregates.
- Cleans comments and filters spam-like or low-information messages.
- Uses zero-shot classification for reliability, value, performance, and comfort aspects.
- Splits longer comments into smaller chunks before scoring to reduce mixed-topic dilution.
- Applies comment weights based on likes and text depth.
- Scores only comments whose `comment_id` has not already been processed unless forced.
- Derives an overall score from confidence-weighted supported aspects.
- Rebuilds `make_sentiment_index` and cumulative point-in-time `make_sentiment_monthly` from the persistent `youtube_comments_scored` table after each run.

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
- Candidate models include Ridge, ElasticNet, LightGBM, and RandomForest, plus median and plain Ridge/LightGBM baselines; routed candidates fit everyday/high-value experts.
- Outputs include JSON and Markdown reports plus `.joblib` model artifacts.

`ML/Time_Series_Price.py` trains cohort-level depreciation forecasts.

Important design choices:

- Cohort grain is canonical make, model, model year, and trim; VIN assignment comes from `vehicle_identity` for stability across snapshots.
- Monthly cohort frames are built from price history.
- Features include market index, cohort lags, rolling prices, mileage, volume, NHTSA attributes, optional collected-time NHTSA features and prior-completed-month make sentiment.
- Models forecast one-month depreciation percentages and recursively emit a monthly median-price path up to five years ahead by default.
- The time-series benchmark now includes global ML, SARIMAX, Prophet, and TimesFM model families when optional dependencies are installed and cohorts have enough monthly support.
- The time-series entry point loads `HF_TOKEN` from the repository-root `.env` before importing TimesFM; an existing shell environment value takes precedence and the local file is Git-ignored.
- Backtesting outputs are written as row-level cohort/model/horizon results plus KPI tables with future-price MAE, WAPE, bias, depreciation error, R2, and skill against a no-change baseline.
- Forecast origins use each cohort's latest retained price-history month; normal runs keep high-value histories because `--max-price` defaults to disabled and is only an opt-in sensitivity cap.
- The script uses global models across cohorts to share signal across sparse vehicle segments.
- Global forecast hyperparameters use fixed defaults in both rolling-origin fits and final artifacts; optimization requires nested origin-specific tuning before promotion.

#### Machine Learning Pipeline Diagram

The two modeling entry points share the cleaned vehicle database but use different
observational grains, validation strategies, encoders, and forecast outputs.

```mermaid
flowchart TB
    DB[(CAR_DATA_CLEANED.db)]
    ABSA[(Optional CAR_YOUTUBE_COMMENTS.db<br/>make_sentiment_monthly)]

    subgraph CP[Current-price pipeline - Price_ML_Models.py]
        CP_LOAD[Latest eligible listing snapshot per VIN<br/>Inner join NHTSA enrichment<br/>Optional point-in-time make sentiment join<br/>Chunked Arrow-backed SQLite reads]
        CP_FILTER[Validate positive price and nonnegative mileage<br/>Require canonical make, model, year, and title-derived trim<br/>Deduplicate to one current row per VIN]
        CP_FE[Engineer current-price features<br/>Age, mileage, recency, location, text, keywords,<br/>canonical identity combinations, market and safety signals]
        CP_LEAK[Leakage guard<br/>Remove price, price_band, VIN/date metadata,<br/>NHTSA base price, legacy identity, and audit fields]
        CP_SPLIT{Validation split}
        CP_TIME[Preferred: time cutoff<br/>Remove training VINs appearing in test]
        CP_GROUP[Fallback: GroupShuffleSplit by VIN]
        CP_NUM[Numeric branch<br/>Median imputation and float32 cast]
        CP_LOW[Low-cardinality categorical branch<br/>UNKNOWN imputation and infrequent-aware one-hot encoding]
        CP_HIGH[High-cardinality or identity branch<br/>UNKNOWN imputation and bounded one-hot encoding]
        CP_COMBINE[ColumnTransformer<br/>Combine numeric, one-hot blocks]
        CP_SCALE[Linear candidates only<br/>Sparse-safe StandardScaler]
        CP_MODELS[Ridge | ElasticNet | LightGBM | RandomForest<br/>Plain and routed log-price regressors plus median baseline]
        CP_TUNE[Randomized hyperparameter search<br/>Up to 200k stratified rows; expanding-date CV or grouped fallback<br/>Refit selected settings on the training split]
        CP_ROUTE[Optional high-value router<br/>Training-label classifier for price above 150k<br/>Everyday and high-value experts plus global blend]
        CP_EVAL[Test evaluation<br/>MAE, RMSE, RMSLE, MAPE, R2<br/>Price-band, high-value, make, and year segments]
        CP_SELECT[Select by validation MAE<br/>Independent interval calibration and final test]
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
        TS_LOAD[Price history plus latest listing identity<br/>VIN consensus identity preferred<br/>NHTSA attributes and as-of make sentiment]
        TS_CLEAN[Clean positive dated observations<br/>Optional max-price sensitivity cap<br/>Normalize canonical make, model, year, and trim proxy]
        TS_COHORT[Monthly make-model-year-trim cohorts<br/>One VIN contribution per month; origin-known support]
        TS_FE[Aggregate and engineer cohort-time features<br/>Price, mileage, volume, calendar, market index,<br/>lags, rolling windows, safety, and sentiment]
        TS_TARGET[Origin-safe targets<br/>Future median price and depreciation by horizon<br/>Future target columns never enter model features]
        TS_SUPPORT{At least 50 complete<br/>cohort-month rows?}
        TS_SKIP[Skip horizon and record reason]
        TS_NUM[Numeric branch<br/>Median imputation]
        TS_CAT[Categorical branch<br/>UNKNOWN imputation and unknown-safe ordinal encoding]
        TS_GUARD{At least two training rows<br/>and a varying target?}
        TS_CONST[DummyRegressor mean baseline]
        TS_GLOBAL[Global supervised model<br/>LightGBM when installed<br/>Otherwise HistGradientBoosting]
        TS_TUNE[Fixed default parameters<br/>Identical parameter policy across rolling origins<br/>Final fit on complete observable targets]
        TS_BACKTEST[Expanding rolling-origin backtest<br/>Origin-known targets; recursive multi-step evaluation]
        TS_RECURSE[Recursive global monthly path<br/>Default 60-month forecast]
        TS_LOCAL[Eligible local cohort histories]
        TS_SARIMAX[SARIMAX]
        TS_PROPHET[Prophet]
        TS_TIMESFM[TimesFM]
        TS_LOCAL_BT[Local rolling backtests and future paths]
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

    NHTSA_TEXT[(Optional NHTSA feature sidecar<br/>Collected before observation month)]
    NHTSA_TEXT -. MMY .-> CP_LOAD
    NHTSA_TEXT -. MMY .-> TS_LOAD
    TS_COHORT --> TS_SIMPLE[No-change and drift baselines] --> TS_OUT
    DB --> CP_LOAD
    DB --> TS_LOAD
    ABSA -. prior completed month .-> CP_LOAD
    ABSA -. prior completed month .-> TS_LOAD
```

##### Current-price feature and encoder reference

Predictors are selected by ALLOWED_PRICE_FEATURES and the chosen ablation group.
Generated reports record the actual numeric and categorical columns. All categorical
branches use bounded one-hot encoding; no target statistics enter preprocessing.

| Feature or processing group | Inputs and derived fields | Encoder or transformation |
|---|---|---|
| Canonical identity | `canonical_make`, `canonical_model`, `canonical_year`, `canonical_trim`; derived `trim_proxy`, `make_model_year`, and `make_model_year_trim` | Identity-like categorical columns are forced into the high-cardinality bounded one-hot branch. Only title-derived canonical trim can become a trim predictor. |
| Mileage and age | `mileage`; derived `vehicle_age`, `vehicle_age_squared`, `miles_per_year`, `log_mileage`, `mileage_age_interaction`, `mileage_bucket`, and `model_year_bucket` | Numeric values use median imputation and float32 conversion. Buckets are encoded according to their observed cardinality. |
| Listing time and geography | `loaddate`; derived `listing_recency_days`, `listing_month`, `listing_week`, and two-digit `location_region` from `locationCode` | Numeric calendar fields use median imputation. Location-like categoricals use bounded one-hot encoding. |
| Listing text and state | Available title fields; derived length/word counts, certified/CPO, AWD/4WD, luxury-trim mentions, `pendingSale` and `source_is_marketplace` | Counts and flags use the numeric branch. Raw title metadata is excluded, while derived text features remain eligible. |
| Vehicle, market, safety, and sentiment | Cleaned listing attributes, usable NHTSA attributes, `body_fuel_segment`, `is_ev_or_hybrid`, and the eight optional `sentiment_*` make-level fields from the latest eligible monthly snapshot | Numeric fields use median imputation; categoricals with at most 50 values use one-hot unless identity-like, with bounded one-hot encoding for the remainder too. |
| Low-cardinality categoricals | Non-identity categorical columns with at most 50 observed values | Normalize missing values, impute `UNKNOWN`, then `OneHotEncoder(handle_unknown="infrequent_if_exist", min_frequency=10, max_categories=25)` with sparse float32 output. |
| High-cardinality categoricals | Columns over 50 values plus names containing make/model/trim/manufacturer/segment/title/location tokens | Normalize missing values, impute `UNKNOWN`, then bounded `OneHotEncoder(min_frequency=10, max_categories=25)` and float32 conversion. |
| Linear candidate preprocessing | Combined numeric and one-hot matrix for Ridge and ElasticNet | `StandardScaler(with_mean=False)` preserves sparse compatibility. Tree candidates use the unscaled combined matrix. |
| Excluded leakage and metadata | `price`, `price_band`, `nhtsa_BasePrice`, `nhtsa_BasePrice_source`, VIN/date/title identifiers, legacy/raw trim fields, canonical audit/provenance fields, EPA IDs, and identity agreement flags | Dropped before preprocessing. `price_band` remains diagnostic-only, and the high-value label is created only inside training. |

##### Cohort-depreciation feature and encoder reference

| Feature or processing group | Exact model inputs | Encoder or transformation |
|---|---|---|
| Cohort identity | `make`, `model`, `model_year`, `trim_proxy` | Constant `UNKNOWN` imputation followed by `OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1, encoded_missing_value=-1)`. |
| Vehicle categories | `body_class`, `drive_type`, `fuel_type`, `electrification_level`, (latest seller/source metadata excluded) | Same unknown-safe ordinal encoding as cohort identity. |
| Price and mileage state | `median_price`, `avg_price`, `price_p25`, `price_p75`, `avg_mileage`, `median_mileage`, `avg_vehicle_age_months`, `avg_miles_per_year` | Median imputation. These values are observed at the forecast origin, not future targets. |
| Volume and calendar | `volume`, `unique_vins`, `price_down_rate`, `month`, `quarter`, `cohort_month_number`, `cohort_age_months` | Median imputation. |
| Cohort trajectory | `cohort_first_median_price`, `price_index_vs_cohort_first`, `cumulative_depreciation_pct`, `lag_median_price_1`, `lag_median_price_2`, `lag_price_index_1`, `rolling_median_price_3m`, `rolling_avg_mileage_3m`, `rolling_volume_3m`, `rolling_depreciation_pct_3m` | Median imputation; lag and rolling values are constructed from observations available at the forecast origin. |
| Market context | `market_median_price`, `market_price_index`, `market_monthly_volume` | Median imputation. |
| Sentiment, powertrain, and safety | `sentiment_overall_score`, four `sentiment_{aspect}_score` fields, `sentiment_comment_count`, `sentiment_video_count`, `sentiment_aspect_coverage`, `engine_hp`, `engine_cylinders`, optional collected-time `nhtsa_*` sidecar fields | Median imputation; monthly make sentiment uses the previous completed month and remains optional when source data is unavailable. |
| Targets and excluded leakage | `target_depreciation_pct_{horizon}m` is the supervised target; `target_median_price_{horizon}m` is retained for evaluation. `nhtsa_BasePrice`, `nhtsa_BasePrice_source`, and all future target columns are excluded from predictors. | Target rows are aligned by future cohort month. Rolling-origin validation restricts training to targets observable by each origin. |

`ML/Model_Output.ipynb` reads generated reports and presents a KPI-style model summary.

### Streamlit Dashboard

`streamlit_app.py` provides an interactive UI over the cleaned database and generated model artifacts. Filters and primary labels use canonical identity; raw titles and NHTSA trims remain visible for comparison. The app reports normalization coverage, EPA matching, unresolved titles, and NHTSA identity disagreement, warns on a missing canonical schema, and disables predictions when database and model normalization versions differ. Current-price joblib artifacts retain compatibility with direct-script training through an explicit custom-object registration layer. Filter-scoped scoring also preserves its metric schema when individual models fail, allowing the dashboard to show model-specific diagnostics without masking them behind a secondary table error.

The Vehicle estimator tab builds a target-free scenario row from user-known make,
model, year, trim, mileage, and optional observed transmission/engine profiles.
All other current-price inputs are resolved from deterministic modal values in
the latest-per-VIN same-year cohort, with exact-trim selection preferred and a
same-year fallback labeled when necessary. The resolved values and support counts
are displayed before all available current-price artifacts are scored. Custom
depreciation paths use the global one-month cohort model with both the predicted
current-price and observed cohort-median anchors over a selectable 12- to
60-month horizon. Saved SARIMAX, Prophet, and TimesFM outputs remain separate
cohort reference paths because they cannot incorporate the custom scenario
overrides.

## Validation and Testing

Recommended validation commands:

```powershell
python Utilities\health_check.py
python Utilities\verify_schema.py
python -m unittest tests\test_ml_upgrade.py
python -m unittest tests\test_nhtsa_enrichment.py
python -m unittest tests\test_vehicle_normalization.py
python -m py_compile DataPipeline\Playwright_test.py DataPipeline\DataAquisition.py DataPipeline\DataCleaning.py DataPipeline\VehicleNormalization.py DataPipeline\NHTSA_enrichment.py DataPipeline\SentimentAnalysis.py DataPipeline\absa_pipeline.py ML\Price_ML_Models.py ML\Time_Series_Price.py Utilities\health_check.py Utilities\verify_schema.py
```

`tests/test_ml_upgrade.py` currently checks:

- Cleaned output preserves key predictive listing fields and indexes.
- Current-price train/test splitting has no VIN overlap.
- Price-history gap loading correctly labels duplicate-like trajectories.

`tests/test_nhtsa_enrichment.py` checks the documented batch payload, complete
raw-field persistence, two-step safety ratings, full recall/complaint storage,
and compatibility projection behavior using mocked responses.

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
python DataPipeline\NHTSA_enrichment.py --resume --refresh-days 30 --rate-limit-delay 1.0
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
python DataPipeline\absa_pipeline.py --migrate-make-grain
python DataPipeline\absa_pipeline.py --run-all --limit 1000
python DataPipeline\absa_pipeline.py --run-all --force-reprocess
```

Current-price modeling:

```powershell
python ML\Price_ML_Models.py --sample-size 5000 --absa-db-path CAR_DATA_OUTPUT\CAR_YOUTUBE_COMMENTS.db
# Explicit full-data run (also the default when the flag is omitted):
python ML\Price_ML_Models.py --sample-size 0
```

`python ML\Price_ML_Models.py --task all` passes the current-price sample size
through to depreciation. With the default `0`, both workflows use their full
eligible data; pass a positive sample size for a bounded end-to-end run.

Current-price plus depreciation modeling:

```powershell
python ML\Price_ML_Models.py --task all --sample-size 5000 --absa-db-path CAR_DATA_OUTPUT\CAR_YOUTUBE_COMMENTS.db
```

Depreciation forecasting only:

```powershell
python ML\Time_Series_Price.py
```

Streamlit dashboard:

```powershell
streamlit run streamlit_app.py
```

## Modeling Roadmap and NHTSA Text Workflow

Updated 2026-09-16. The implementation below replaces the earlier deferred plan.
The long ABSA worker was stopped at the user's request; its follow-up automation
is paused. Existing committed scores are preserved. No production inference or
full model training was run to validate these code changes. Final verification:
82 unit tests passed with Hugging Face offline; Python compilation, all 13
notebook code-cell syntax checks, CLI help checks and git diff whitespace checks
passed. Synthetic fixtures exercised the pilot/cache/build/review workflow and
model reports; these checks do not establish real-data predictive lift.

### Implemented safeguards and research corrections

- Current price uses distinct valid dates to choose temporal splits, removes VIN
  overlap, and records grouped fallback when date support is insufficient. An
  explicit unsupported cutoff raises an error. Separate train, validation,
  calibration, and test partitions prevent test-based model selection.
- Age uses listing observation year. The compatibility name listing_recency_days
  now means days since 2000-01-01, a fixed calendar reference. Old artifacts must
  be retrained; dashboard inference checks feature contract pit-v1.
- A reviewed predictor allowlist excludes source identifiers, operational fields,
  base prices filled from observed prices, latest recall/complaint projections,
  and raw identity diagnostics. Static decoded equipment remains eligible.
- Bounded one-hot encoding replaces in-sample target encoding. This is a simpler
  target-independent baseline; temporal/group cross-fitted target encoding remains
  an optional later experiment, not an unverified requirement for improvement.
- Median, plain Ridge, and plain LightGBM compete with the existing four routed
  candidates. Tuning and selection use MAE. Tuning prefers expanding-date folds
  with VIN exclusion, with grouped fallback. The independent calibration split
  supplies 90% marginal residual intervals when at least 30 rows exist; report
  test coverage and width. Temporal drift prevents a coverage guarantee.
- Forecast data contributes only the last retained observation per VIN/month.
  Targets and lags use exact calendar months; absent targets remain absent.
  Mileage is forward-filled within VIN only. Optional missing features are
  imputed rather than causing all rows to be deleted. Latest seller/source
  metadata is excluded from historical predictors.
- Cohort support is measured at the origin. Local models fill gaps using only
  their own training prefix and evaluate observed actuals only. Global rolling
  fits and final artifacts use fixed defaults with origin-known target dates, avoiding parameters
  selected on later periods. One-month recursive backtests evaluate multiple
  future months with the same recursive feature updates used in deployment.
- Monthly recursion requires a one-month fitted model. Price lags, rolling price
  state, price indices, and vehicle age advance; price quantiles retain relative
  spreads. Mileage, market state, volume, and external evidence are held fixed
  scenarios. Unsupported five-year horizons are extrapolations, not validated
  forecasts. No future recall, complaint, or sentiment values are supplied.
- No-change and calendar-drift baselines appear in forecast/backtest CSVs.
  cohort_backtesting_matched_kpis.csv additionally compares common cases across
  the model methods present at each horizon. Local-family cohort caps still
  limit the evaluated population; do not generalize those results to all makes.

### NHTSA sources, grain, caching, and joins

DataPipeline/NHTSA_text_features.py is a separate source-specific pipeline. It
reads CAR_DATA_NHTSA.db without modifying it and writes the optional derived
CAR_NHTSA_TEXT_FEATURES.db sidecar. This avoids migrating the large source store.

| Source | Text roles | Implemented features | Join grain |
| --- | --- | --- | --- |
| YouTube | Existing opinion aspects | Only overall, reliability, value, performance, comfort, comment/video counts, aspect coverage | Canonical make; latest completed publication month |
| Complaints | summary | Distinct report count; known flag; scored-text coverage; structured crash/fire/injury/death report shares; propulsion/control loss, recurring failure, repair delay topic scores | Exact normalized make/model/model year from query_id |
| Recalls | summary plus consequence; remedy separately | Distinct campaign count; known flag; coverage; potential propulsion/control/fire hazards and software/replacement remedy scores | Exact normalized query make/model/model year |

ODI number and campaign number deduplicate events within a query association;
record_key is the fallback when an official identifier is absent. Component rows
contribute unique text to the same event. The same report can legitimately belong
to multiple MMY associations. Text reuse does not merge distinct events. Masked
complaint VINs are not listing VIN join keys; manufacturer is not a make key.
The implemented crosswalk only normalizes case/punctuation/spacing. No fuzzy or
unreviewed model aliases are inferred; unmatched associations remain missing.

Scores are cached by normalized text hash and field role inside a sidecar pinned
to a single model commit and taxonomy/chunking/hypothesis version. A different
model/revision requires a separate sidecar. Inference covers overlapping token
chunks; max pooling identifies topic evidence, not calibrated probabilities or
validated evidence spans. No raw response JSON or copied narratives are stored
in the sidecar. Pilot CSV exports contain source text for human annotation; the
review command exports that pilot with cached scores. Existing pilot/review CSVs
are protected from overwrite; choose another output path for a new export.
NHTSA text uses neither YouTube spam filters nor popularity weighting.

The strict join uses the most recent retained query collected *before* the start
of the listing/history month, separately for complaints and recalls, and never
multiplies source model rows. Failure stays unknown; confirmed empty is zero
reports. Text scores remain missing until all available event texts in that
query have been scored. Coverage and report counts remain inspectable. Severity
shares use only reports with known corresponding structured flags/counts; they
are report shares, not vehicle failure rates. There is no exposure denominator.

Collection timestamps, not filing/incident/report dates, control this version.
The inspected retained query history starts 2026-08-27, so early historical
observations cannot obtain strict NHTSA text features. Deduplicated query hashes
retain the first stored timestamp and do not log every refresh. These joins are
collected-data reconstructions, not complete archives of every API state.
Older filing dates do not prove that today's narrative/remedy was available then.
An event-date retrospective mode and trailing 3/12-month activity features were
removed from the initial implementation because date-format/version and coverage
assumptions are not established. Do not label acquisition bursts as new defects.

### Evaluation gates and remaining research

The CLI feature groups are baseline (default), youtube, nhtsa-structured, nhtsa,
and all. Each retains the same fundamentals and split rules; nhtsa adds text to
structured NHTSA, while all additionally adds YouTube. Save each run separately.
Missing optional sources do not prevent baseline modeling, but coverage must be
checked before interpreting an enriched experiment. Frozen input databases are
needed for identical comparison cases across separate runs.

Pilot extraction is bounded and samples unique text within each role, in source
order. It is a starting annotation queue, not a representative evaluation set.
Supplement with make/year/time, long-text, negation and rare-label strata; split
by campaign/ODI family, double-label a subset, and reserve a held-out test set.
Compare pinned BART-MNLI and an appropriately pinned DeBERTa model using per-label
precision/recall/F1, abstention and throughput. Calibrate or select thresholds on
development labels only. Human annotation and real held-out experiments have
not been performed, so neither NLP lift nor a best NLP model is claimed.

Optional later work remains: validated alias mappings and conflict audits;
source-date reconstruction with verified formats; coverage-denominated recent
activity windows; variant-resolved historical crash ratings; matched-VIN versus
changing-cohort price changes; dependence-aware uncertainty and supported
forecast intervals; TF-IDF/supervised text baselines; optional Chronos-2. These
require evidence or annotations before promotion. Existing latest crash-rating
projections remain excluded instead of being silently used historically.
Latest canonical identity and cleaned outlier thresholds can themselves contain
hindsight. Price-history event medians do not measure standing inventory, causal
safety effects, or realized sale prices. YouTube publication-time reconstructions
still carry collection/edit/like-count hindsight caveats.

### Reviewed run order (2026-09-16)

Run from PowerShell at the repository root. Stop if any command fails. These are
commands for the user to run later; implementation validation did not execute
inference or production training. No scraper/enrichment refresh is necessary
merely to reuse the existing databases.

```powershell
Set-Location E:\Car-Price-Data-Visualization-Learning
$python = '.\.venv\Scripts\python.exe'
& $python -m unittest tests.test_ml_upgrade tests.test_canonical_backtesting tests.test_sentiment_incremental tests.test_nhtsa_text_features tests.test_streamlit_vehicle_scenario
```

1. Rebuild existing make attribution/aggregates without inference. Then resume
   unprocessed comments using bounded commits. The second command can take hours;
   rerunning it skips persisted comment IDs. Filtered-out raw text may be visited
   again but does not trigger inference. Never add --force-reprocess for this run.

```powershell
& $python DataPipeline\absa_pipeline.py --migrate-make-grain
& $python DataPipeline\absa_pipeline.py --run-all --batch-size 128 --model-revision d7645e127eaf1aefc7862fd59a17a5aa8558b8ce
```

2. Export a small NHTSA annotation queue and build structured-only features.
   These two commands perform no inference. Inspect nhtsa_text_pilot.csv and
   annotate/validate labels before treating text scores as production evidence.

```powershell
& $python DataPipeline\NHTSA_text_features.py pilot --limit 300
& $python DataPipeline\NHTSA_text_features.py build
```

3. When ready for the NLP experiment, score a bounded batch, inspect it, then
   resume all unprocessed unique text and rebuild the derived feature rows.
   --device 0 selects CUDA; use --device -1 for CPU. Full scoring can be long.

```powershell
& $python DataPipeline\NHTSA_text_features.py score --pilot-only --limit 300 --device 0
& $python DataPipeline\NHTSA_text_features.py review
# Review the exported nhtsa_text_pilot_scored.csv before the full experiment.
& $python DataPipeline\NHTSA_text_features.py score --device 0
& $python DataPipeline\NHTSA_text_features.py build
```

4. Compare the five feature groups with frozen inputs and separate artifacts.
   The following are bounded development runs; a later production rerun may use
   --sample-size 0. These are research runs, not evidence of predictive lift until
   reports are reviewed. NHTSA text comparisons need adequate complete coverage.

```powershell
$run = '.\MODELS_OUTPUT\review_' + (Get-Date -Format 'yyyyMMdd_HHmmss')
foreach ($group in @('baseline', 'youtube', 'nhtsa-structured', 'nhtsa', 'all')) {
    $out = Join-Path $run $group
    & $python ML\Price_ML_Models.py --sample-size 5000 --feature-set $group --output-dir $out
    if ($LASTEXITCODE -ne 0) { throw "Current-price run failed: $group" }
    & $python ML\Time_Series_Price.py --sample-size 5000 --feature-set $group --time-series-models global_ml --target-months 1 --forecast-months 60 --output-dir $out
    if ($LASTEXITCODE -ne 0) { throw "Forecast run failed: $group" }
}
```

5. Review reports, source coverage, validation MAE, calibration/test intervals,
   forecast horizon support and matched-case KPIs before choosing a feature group.
   Optional SARIMAX/Prophet/TimesFM runs should use the same frozen input and group.
   The dashboard and notebook accept MODEL_OUTPUT_DIR; select the reviewed run
   instead of overwriting older artifacts.

```powershell
$env:MODEL_OUTPUT_DIR = (Resolve-Path (Join-Path $run 'baseline')).Path
& $python -m streamlit run streamlit_app.py
```

Start the notebook kernel with the same environment variable, or set its
OUTPUT_DIR to the selected run. Old artifacts remain available for reports, but
must be retrained before inference using the changed feature semantics.

### Research checked for this plan

- [NHTSA datasets and APIs](https://www.nhtsa.gov/nhtsa-datasets-and-apis): official
  source scope and make/model/year queries.
- [NHTSA complaint file definition](https://static.nhtsa.gov/odi/ffdd/cmpl/CMPL.txt):
  incident versus received/added dates and repeated ODI numbers across components.
  This bulk specification does not override the API formats observed locally.
- [Automotive aspect sentiment dataset research](https://aclanthology.org/2020.coling-main.83/):
  domain-specific aspect annotation; does not establish transfer to recalls.
- [Automotive complaint language-model research](https://arxiv.org/abs/2012.02558):
  technical complaint understanding; not evidence of vehicle-price improvement.
- [BART-MNLI model card](https://huggingface.co/facebook/bart-large-mnli) and
  [DeBERTa zero-shot model card](https://huggingface.co/MoritzLaurer/deberta-v3-base-zeroshot-v2.0):
  candidate NLI classification approaches; local pilot validation remains required.
- [Target encoder cross-fitting](https://scikit-learn.org/stable/auto_examples/preprocessing/plot_target_encoder_cross_val.html)
  and [rolling-origin evaluation](https://otexts.com/fpp3/tscv.html): validation
  methods underlying the modeling corrections.

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
