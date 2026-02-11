# PROJECT SUMMARY
## Car Market Data Pipeline

Last Updated: February 10, 2026

---

## Executive Overview

This repository implements a production-grade data pipeline that captures vehicle listings from AutoTempest using Selenium with stealth configuration and Chrome DevTools Protocol (CDP) network interception. The pipeline stores raw listings in SQLite, enriches records using the NHTSA vPIC API, and supports downstream cleaning and analysis. The system prioritizes data integrity, performance stability, and operational resilience.

Key objectives:
- Capture listing data via API interception rather than HTML parsing.
- Maintain consistent iteration performance during long-running sessions.
- Preserve data integrity through schema validation and transactional inserts.
- Provide a clear, operationally focused workflow for data acquisition and enrichment.

---

## Repository Layout

```
Car-Price-Data-Visualization-Learning/
├── DataPipeline/
│   ├── DataAquisition.py
│   ├── database.py
│   ├── DataCleaning.py
│   ├── NHTSA_enrichment.py
│   └── migrate_to_db.py
├── Utilities/
│   ├── health_check.py
│   ├── verify_schema.py
│   └── fix_database_schema.py
├── CAR_DATA_OUTPUT/
│   ├── CAR_DATA.db
│   ├── scraping_*.log
│   └── nhtsa_enrichment_*.log
├── requirements.txt
├── PROJECT_SUMMARY.md
└── README.md
```

---

## Quick Start

```bash
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
```

---

## Architecture and Data Flow

1. Data acquisition uses Selenium with stealth configuration to load AutoTempest and trigger source-specific pagination buttons.
2. CDP performance logs are parsed to capture `queue-results` API responses.
3. Listings are normalized and inserted into SQLite via `database.py` with transactional batch writes.
4. Unenriched VINs are sent to NHTSA vPIC endpoints for specifications, safety ratings, recalls, and complaints.
5. Data cleaning provides normalization for price, mileage, location, and time fields for analysis.

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

Schema validation:
- Use `Utilities/verify_schema.py` to verify the current database structure.
- If out of sync, run `Utilities/fix_database_schema.py` to apply additive migrations.

---

## Performance Stabilization

Observed issue:
- Iterations degraded from ~19 seconds to 60+ seconds after 100+ iterations.

Root causes:
- CDP performance log accumulation and browser memory growth.

Fixes implemented:
- Clear CDP performance logs every iteration.
- Periodically restart the browser driver to reset memory usage.
- Batch database inserts with a single transaction per iteration.

Result:
- Consistent iteration times across long-running sessions.

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

### Schema Validation
```bash
python Utilities\verify_schema.py
```

---

## Configuration

Edit the `Config` class inside `DataPipeline/DataAquisition.py` to adjust:
- ZIP and radius
- Minimum model year
- Headless mode
- Retry limits and randomized delays
- Driver restart interval

---

## Troubleshooting

1. Schema mismatch error
   - Run `python Utilities\verify_schema.py`.
   - If required columns are missing, run `python Utilities\fix_database_schema.py`.

2. Selenium slowdown
   - Confirm log cleanup and periodic restart are enabled in `DataAquisition.py`.

3. Database locked
   - Ensure a single writer is active; SQLite supports one writer at a time.

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

- Consolidated documentation into a single `PROJECT_SUMMARY.md` and a concise `README.md`.
- Moved utility scripts to `Utilities/` for separation from pipeline code.
- Removed redundant documentation and backups to keep the repository minimal.
