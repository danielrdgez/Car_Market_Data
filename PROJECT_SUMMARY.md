# PROJECT SUMMARY
## Car Market Data Pipeline

Last Updated: March 5, 2026

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

1. For each vehicle make, the orchestrator launches a `ButtonScrapingCoordinator` that spawns one dedicated browser instance per source button (autotempest, hemmings, cars, etc.) in parallel threads.
2. Each `ButtonScraper` thread clicks its assigned button repeatedly until exhaustion (3 consecutive zero-row responses or button no longer clickable).
3. CDP performance logs are parsed per-button to capture `queue-results` API responses.
4. A thread-safe `VINCache` prevents duplicate inserts across all concurrent button workers. The database layer uses thread-local connections with a shared write lock.
5. Listings are normalized and inserted into SQLite via `database.py` with transactional batch writes.
6. Unenriched VINs are sent to NHTSA vPIC endpoints for specifications, safety ratings, recalls, and complaints.
7. Data cleaning provides normalization for price, mileage, location, and time fields for analysis.

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

Schema validation:
- Use `Utilities/verify_schema.py` to verify the current database structure.
- If out of sync, run `Utilities/fix_database_schema.py` to apply additive migrations.

---

## Performance Stabilization

Observed issue:
- Single-browser button cycling caused slowdowns as DOM grew from repeated clicks across all sources. Buttons with large result sets (cars, other) degraded the shared browser instance.

Fixes implemented:
- Per-button browser isolation: each button runs in its own Chrome instance, preventing DOM bloat from affecting other buttons.
- Parallel button execution via `ThreadPoolExecutor` with configurable concurrency (default 4 workers per make).
- Clear CDP performance logs every iteration per button.
- Thread-safe VIN cache with database-backed deduplication prevents redundant inserts across threads.
- No artificial click limits; buttons run to true exhaustion (3 zero-row strikes).

Result:
- Consistent per-button iteration times regardless of other buttons' progress.
- Faster overall completion since slow buttons (cars, other) no longer block fast buttons.

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

- Refactored DataAquisition.py to per-button parallel architecture: each source button gets a dedicated browser instance running in its own thread, eliminating DOM bloat slowdowns.
- Added thread-safe VINCache and database write locking for concurrent button workers.
- Removed click limits and driver restart logic; buttons now run to true exhaustion via strike counting.
- Updated database.py with thread_safe mode using thread-local connections and a shared write lock.
- Consolidated documentation into a single `PROJECT_SUMMARY.md` and a concise `README.md`.
- Moved utility scripts to `Utilities/` for separation from pipeline code.
