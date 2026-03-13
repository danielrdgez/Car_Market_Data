# AGENTS Guide

## Mission-Critical Context
- This repo is a Python pipeline: scrape AutoTempest via network interception, store snapshots in SQLite, then enrich VINs with NHTSA APIs.
- Core flow: `DataPipeline/DataAquisition.py` -> `DataPipeline/database.py` -> `DataPipeline/NHTSA_enrichment.py` -> optional cleanup in `DataPipeline/DataCleaning.py`.
- Preserve the key design: intercept `queue-results` JSON via CDP `performance` logs; do not switch to HTML parsing.

## Architecture To Learn First
- `ParallelScrapingOrchestrator` iterates makes sequentially; `ButtonScrapingCoordinator` runs source buttons in parallel.
- Each `ButtonScraper` uses its own Chrome instance (`create_stealth_driver`) to avoid shared-DOM slowdown and detection coupling.
- Button completion is behavioral: stop after `EXHAUSTION_STRIKE_COUNT` consecutive zero-row API responses.
- Dedup is two-layered: in-memory `VINCache` plus DB-sourced `get_seen_vins()` for current `loaddate`.
- Acquisition writes use `CarDatabase(thread_safe=True)` with thread-local SQLite connections and a shared write lock.

## Runbook Commands
```powershell
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
python Utilities\verify_schema.py
python Utilities\fix_database_schema.py
```

## Coding Rules Specific To This Repo
- Stealth-first Selenium is mandatory (`selenium-stealth`, automation flags disabled, custom user-agent in headless mode).
- Use randomized waits (`random.uniform(...)`) for automation timing; avoid fixed sleeps.
- Keep ingestion values normalized (`normalize_price`) and validate VINs before enrichment (`_is_valid_vin`).
- Keep all NHTSA-enriched fields prefixed `nhtsa_`.
- Prefer incremental persistence (batch inserts per intercepted API payload) over large in-memory accumulation.
- For data cleaning work, prefer `polars` as the default dataframe engine going forward.
- Keep code professional and readable: no emojis and no excessive comments (comment only where logic is non-obvious).

## Documentation And Dependency Hygiene
- Do not create extra markdown docs unless requested; update existing docs first (`README.md`, `PROJECT_SUMMARY.md`, `.github/*.md`).
- Keep markdown professional and emoji-free.
- When adding new packages, update `requirements.txt` in the same change.

## Integrations And Boundaries
- External integrations: AutoTempest backend `queue-results` via CDP; NHTSA vPIC + SafetyRatings + Recalls + Complaints APIs.
- Persistence boundary: `CAR_DATA_OUTPUT/CAR_DATA.db`; operational logs: `CAR_DATA_OUTPUT/*.log`.
- `NHTSAEnricher` processes VINs in batches (`MAX_BATCH_SIZE=50`) and caches MMY-level lookups.

## Gotchas
- Keep direct script execution intact (`if __name__ == "__main__": main()`).
- `DataPipeline` imports `database` as a sibling module; avoid refactors that break root-level script runs.
- `Utilities/verify_schema.py` and `Utilities/fix_database_schema.py` use absolute DB paths.
- `DataCleaning.py` still contains machine-specific default paths; treat it as non-production wired.

