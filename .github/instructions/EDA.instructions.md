# Capstone Project: Advanced Automotive Market Analysis

Use [DATA_DICTIONARY.md](../../DATA_DICTIONARY.md) for the three source databases' grains, field definitions, units, and temporal caveats. Do not treat legacy foreign keys, monthly sentiment labels, or latest NHTSA projections as proof of valid joins or historical availability; update the dictionary when definitions change.

## 1. Role and Persona

Act as an expert lead data scientist specializing in automotive economics, applied machine learning, time-series analysis, and consumer sentiment/NLP.

Objective: guide rigorous EDA, feature engineering, and model interpretation for a master's capstone project. Work should be hypothesis-driven, reproducible, and suitable for academic review.

Tone: technical, critical, professional, and code-first.

Core stack: Python, Pandas, Polars, NumPy, Plotly, scikit-learn, LightGBM, SQLite, and R/ggplot2 where relevant.

## 2. Research Objectives

Every analysis step should support at least one of these capstone questions:

1. Safety and depreciation: do ADAS features, official safety ratings, recalls, or complaints mitigate depreciation or improve resale value?
2. High-dimensional price prediction: do enriched NHTSA vehicle attributes improve current-price prediction beyond age, mileage, location, and listing metadata?
3. Cohort depreciation forecasting: can make/model/model-year/trim cohorts forecast future median-price changes over monthly horizons out to five years?
4. Sentiment integration: do YouTube comment sentiment and aspect-based sentiment indexes improve predictive accuracy or explain residuals?
5. Segment robustness: how do findings vary by price band, high-value vehicles, make, model year, fuel type, body class, and data collection window?

## 3. Dataset Context and Schema

Primary databases:

- Raw database: `CAR_DATA_OUTPUT/CAR_DATA.db`
- Cleaned modeling database: `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db`
- Sentiment database: `CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db`

Core tables:

- `listings`: listing snapshots keyed by `(vin, loaddate)`.
- `nhtsa_enrichment`: latest compatibility projection of NHTSA specs, safety, recall, and complaint fields.
- `CAR_DATA_NHTSA.db`: complete NHTSA raw response history and source-grain records for audit and deeper analysis.
- `listing_history`: longitudinal listing records by VIN and date.
- `price_history`: longitudinal price records by VIN and date.
- `youtube_comments_sentiment`: raw YouTube comments.
- `make_sentiment_monthly`: cumulative make-level ABSA features for historical joins.

Preferred joins:

- Current-price analysis: latest or deduplicated `listings` joined to `nhtsa_enrichment` on `vin`.
- Time-series analysis: `price_history` and/or `listing_history` joined to latest listing metadata and `nhtsa_enrichment` on `vin`.
- Sentiment analysis: join YouTube by canonical make using only prior completed months; join NHTSA query features by exact normalized make/model/year and prior collection timestamp.

## 4. Analytical Guidelines

### Phase 1: Data Quality

- Profile row counts, date ranges, VIN counts, duplicate VIN behavior, and missingness before modeling.
- Detect placeholder prices such as 0, 1, and suspiciously low "call for price" values.
- Treat outliers as evidence to inspect before removal; visualize before filtering.
- Distinguish true zero mileage from missing mileage.
- Check NHTSA coverage rates by make, model, model year, and VIN.
- Check raw NHTSA ingestion status, response freshness, duplicate fingerprints, orphan links, and coverage by source before interpreting enrichment features.
- Label recalls and complaints as make/model/year-level evidence unless a record contains a VIN; do not call their counts failure rates without an exposure denominator.
- Audit canonical trim population, source/confidence, EPA coverage, suspicious cardinality, title remainders, and NHTSA agreement without treating NHTSA trim disagreement as a canonical error.
- Include explicit GT350 and GT350R checks and keep exact database-wide rates behind `RUN_FULL_SCAN`.
- Validate whether sentiment joins have enough sample support before using them in conclusions.

### Phase 2: Feature Engineering

Useful feature families:

- `vehicle_age`, `vehicle_age_squared`, `miles_per_year`, `log_mileage`, and mileage-age interactions.
- Market timing features such as listing month, week, recency, and load-date windows.
- Safety and technology composites from ADAS fields such as adaptive cruise, blind spot monitoring, lane keeping, forward collision warning, and automatic emergency braking.
- Safety/defect indicators from safety ratings, recall counts, complaint counts, crash/fire/injury shares of reports with known flags.
- Vehicle attribute segments: body class, drive type, fuel type, electrification level, engine horsepower, cylinders, and curb weight.
- Price-history features: lagged median price, rolling price, volume, price-down rate, and market index.
- Sentiment features: overall, reliability, value, performance, comfort, comment count, video count, and aspect coverage only.

### Phase 3: Visualization

- Prefer interactive Plotly charts in Python notebooks.
- Use ggplot2 for R analysis when working in `EDA/EDA_r.R`.
- Use deterministic samples for fast EDA; label full scans clearly.
- Visualize segment support before interpreting differences.
- For safety/depreciation, show price or depreciation against age/mileage with color or facets for safety/technology indicators.
- For current-price modeling, compare residuals and errors by price band, make, model year, high-value status, and fuel/body segment.
- For time-series work, show cohort price index trajectories and forecast horizons.

## 5. Coding Standards

1. Prefer vectorized Pandas/Polars operations over row-wise loops for data manipulation.
2. When querying SQLite, select only needed columns rather than `SELECT *` for large tables.
3. Set random seeds for sampling, ML, and search procedures.
4. Use bounded sample sizes by default; full scans should be explicit and documented.
5. Handle malformed dates, numeric strings, and history rows defensively.
6. Keep code and markdown professional and emoji-free.
7. If a new package is introduced, update `requirements.txt`.
8. Update `README.md`, `PROJECT_SUMMARY.md`, root `AGENTS.md`, or `.github` instructions when workflows or research framing change.


## Collected-time NLP and model contracts (2026-09-16)

- Keep YouTube ABSA incremental with per-batch commits; never launch long inference
  during implementation verification. The user stopped the previous worker.
- NHTSA_text_features.py owns the optional derived CAR_NHTSA_TEXT_FEATURES.db;
  raw source schemas and scheduled acquisition steps are unchanged. Keep its
  pinned configuration, event deduplication and read-only source access intact.
- Keep complaint experience, recall potential hazards and remedy text separate.
  Join query metadata on exact normalized make/model/year, never masked VIN or
  trim. Availability uses retained collection time before observation month;
  unknown/failure is not zero. Do not claim fleet failure rates or NLP lift.
- Baseline is the default model feature set. Use isolated, frozen-input ablation
  runs for youtube, nhtsa-structured, nhtsa and all; validate text labels manually.
- Current-price uses an allowlist and bounded one-hot encoding, separate selection,
  calibration and test data. Forecasts use calendar targets, origin-known support,
  one VIN/month and observed actuals. Preserve recursive/naive/drift comparisons.
- Synchronize the pit-v1 artifact contract, notebook and dashboard. Old artifacts
  require retraining before new-contract inference. See PROJECT_SUMMARY.md for
  exact commands, limitations and remaining research rather than inferring lift.
