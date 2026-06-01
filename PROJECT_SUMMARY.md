# PROJECT SUMMARY
## Car Market Data Pipeline

Last Updated: June 1, 2026

---

## Executive Overview

This repository implements a production-grade data pipeline that captures vehicle listings from AutoTempest using Selenium with stealth configuration and Chrome DevTools Protocol (CDP) network interception. The pipeline enriches records with official NHTSA specifications and incorporates YouTube sentiment analysis for market sentiment tracking. It features an automated machine learning pipeline for price prediction and comprehensive EDA tools to analyze vehicle depreciation.

Key objectives:
- Capture listing data via API interception rather than HTML parsing.
- Enrich vehicle metadata with NHTSA vPIC API (Specs, Safety, Recalls).
- Integrate NLP/Sentiment analysis on vehicle reviews and listing descriptions.
- Predict residual values using advanced ML models (Random Forest, HistGradientBoosting).
- Preserve data integrity through schema validation and transactional inserts.

---

## Repository Layout

```
Car-Price-Data-Visualization-Learning/
├── DataPipeline/
│   ├── DataAquisition.py
│   ├── database.py
│   ├── DataCleaning.py
│   ├── NHTSA_enrichment.py
│   ├── SentimentAnalysis.py  # YouTube API integration
│   └── migrate_to_db.py
├── EDA/
│   ├── EDA_notebook.ipynb    # Interactive Plotly analysis
│   ├── Depreciation_Analysis.py
│   └── EDA_r.R               # R/ggplot2 analysis
├── ML/
│   ├── Price_ML_Models.py    # Training pipelines
│   └── Model_Output.ipynb    # Model validation
├── MODELS_OUTPUT/            # Serialized models (.joblib)
├── Utilities/
│   ├── health_check.py
│   ├── verify_schema.py
│   └── fix_database_schema.py
├── CAR_DATA_OUTPUT/
│   ├── CAR_DATA.db
│   ├── CAR_DATA_CLEANED.db
│   └── *.log
├── requirements.txt
├── PROJECT_SUMMARY.md
└── README.md
```

---

## Research Objectives

The project is guided by three core research questions:
1. **Safety & Depreciation:** To what extent does active safety technology (ADAS) and official safety ratings mitigate depreciation rates?
2. **High-Dimensional Prediction:** Can we improve 5-year residual value prediction using rich NHTSA vehicle attributes?
3. **Sentiment Integration:** Does the integration of NLP on listing descriptions and consumer reviews (YouTube) improve price prediction accuracy?

---

## Quick Start

```bash
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py
python DataPipeline\SentimentAnalysis.py --video-id [VIDEO_ID]
python ML\Price_ML_Models.py
```

---

## Architecture and Data Flow

1. **Acquisition:** Orchestrator launches parallel `ButtonScrapers` with Selenium Stealth. API responses are intercepted via CDP.
2. **Deduplication:** A thread-safe `VINCache` prevents duplicate inserts across concurrent workers.
3. **Enrichment:** Unenriched VINs are processed through the NHTSA vPIC API for specs, safety ratings, and recalls.
4. **Sentiment Analysis:** The `SentimentAnalysis.py` module fetches transcripts and comments for specific vehicle-related YouTube videos.
5. **Cleaning:** `DataCleaning.py` (Polars-based) normalizes price, mileage, and location fields.
6. **Modeling:** `Price_ML_Models.py` trains various regressors (Elastic Net, Random Forest, HGBR) to predict prices based on enriched specs.

Button execution order places the slowest sources (cars, other) last in the FIFO queue so faster buttons finish while heavy buttons continue in parallel.

---

## Core Design Principles

- Stealth first: avoid bot detection through stealth configuration and randomized timing.
- API interception over HTML parsing: capture structured JSON directly from network traffic.
- Data integrity: enforce consistent schemas and write operations with transactions.
- Operational stability: reduce memory growth and sustain iteration speed.

---

## Database Schema Summary

Primary table: `listings`
- Composite primary key: `(vin, loaddate)` for time-series snapshots.
- Columns include `year`, `title`, `details`, `price`, `mileage`, location, seller metadata, and source identifiers.

Supporting tables:
- `price_history` for price trajectory.
- `listing_history` for mileage and availability changes.
- `nhtsa_enrichment` for static vehicle specifications and safety metadata.
- YouTube data is currently exported to CSV in `CAR_DATA_OUTPUT/` for downstream sentiment processing.

Schema validation:
- Use `Utilities/verify_schema.py` to verify the current database structure.
- If out of sync, run `Utilities/fix_database_schema.py` to apply additive migrations.

---

## Performance and Modeling

### Scraping Stabilization
- Per-button browser isolation prevents DOM bloat.
- Thread-safe write locks ensure database integrity during high-concurrency scraping.

### Machine Learning
- **Models:** Elastic Net, Random Forest, Hist Gradient Boosting.
- **Preprocessing:** Automatic handling of categorical features (OHE/Ordinal) and numeric scaling.
- **Validation:** Grouped 80/20 train/test split (grouped by Make-Model-Year) to ensure robust evaluation.

---

## Operational Workflow

### Data Acquisition
```bash
python DataPipeline\DataAquisition.py
```

### NHTSA Enrichment
```bash
python DataPipeline\NHTSA_enrichment.py
```

### Data Cleaning
```bash
python DataPipeline\DataCleaning.py
```

### Sentiment Analysis
```bash
python DataPipeline\SentimentAnalysis.py --video-id [ID]
```

### Machine Learning Training
```bash
python ML\Price_ML_Models.py
```

### Schema Validation
```bash
python Utilities\verify_schema.py
```

---

## Configuration

Edit the `ScrapingConfig` dataclass inside `DataPipeline/DataAquisition.py` to adjust:
- ZIP code and state/localization scope
- Makes list
- Max concurrent button workers per make
- Exhaustion strike count (zero-row clicks before a button is marked done)
- Headless mode
- Retry limits and randomized delays

---

## Troubleshooting

1. Schema mismatch error
   - Run `python Utilities\verify_schema.py`.
   - If required columns are missing, run `python Utilities\fix_database_schema.py`.

2. Selenium slowdown
   - Each button has its own browser instance. If a single button is slow, it does not affect others.
   - Confirm log cleanup is enabled (LOG_CLEANUP_INTERVAL in ScrapingConfig).

3. Database locked
   - The scraper uses thread-safe mode with a write lock for concurrent button threads. External tools should not write to the database during scraping.

4. NHTSA API returns empty results
   - Validate VINs before enrichment and apply retries with backoff.

---

## Data Quality Notes

- Price fields should be normalized to numeric values during cleaning.
- VINs must be validated prior to enrichment to avoid unnecessary API calls.
- All writes should be append-only when exporting to CSV to prevent data loss.

---

## Testing Checklist

1. Run `python Utilities\health_check.py` and resolve any failures.
2. Run `python DataPipeline\DataAquisition.py` and confirm rows are inserted.
3. Run `python DataPipeline\NHTSA_enrichment.py` and confirm enrichment rows increase.

---

## Change Log

- **June 1, 2026:** Integrated Sentiment Analysis and Machine Learning pipelines into core documentation.
- **May 21, 2026:** Migrated DataCleaning to Polars/Pandas hybrid and updated NHTSA retry logic.
- **Refactored DataAquisition.py** to per-button parallel architecture: each source button gets a dedicated browser instance running in its own thread, eliminating DOM bloat slowdowns.
- **Added thread-safe VINCache** and database write locking for concurrent button workers.
- **Updated database.py** with thread_safe mode using thread-local connections and a shared write lock.
- **Consolidated documentation** into `PROJECT_SUMMARY.md` and `README.md`.
- **Moved utility scripts** to `Utilities/`.
