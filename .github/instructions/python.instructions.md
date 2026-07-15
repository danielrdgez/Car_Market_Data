---
applyTo: "**/*.py"
---

# Python and Scraper Guidelines: Automotive Market ML Capstone

## 1. Global Logic and Strategy

Goal: collect automotive market data as structured network payloads, enrich it, clean it, and model it with research-grade validation.

Core rule: use API/network interception for vehicle listing data. Do not replace structured `queue-results` capture with HTML parsing.

## 2. Active Scraper Path

The active scraper is `DataPipeline/Playwright_test.py`.

Preserve these patterns:

1. Use Playwright response interception for `queue-results` fetch/XHR responses.
2. Keep the global task queue over `(make, source button)` combinations.
3. Keep bounded global browser concurrency through `MAX_GLOBAL_CONCURRENT_BROWSERS`.
4. Stagger browser startup to avoid CPU, RAM, and network spikes.
5. Use randomized waits between clicks and iterations.
6. Persist rows incrementally through `CarDatabase(thread_safe=True)`.
7. Keep `VINCache` aligned with database deduplication.
8. Stop a button after `EXHAUSTION_STRIKE_COUNT` consecutive zero-row responses.

Do not make scraper behavior more aggressive without adding stability safeguards and documenting the reason.

## 3. Selenium Reference Path

`DataPipeline/DataAquisition.py` is the legacy/reference Selenium CDP scraper.

If touching this file:

1. Preserve `selenium-stealth`.
2. Preserve Chrome DevTools Protocol performance logging for `queue-results`.
3. Keep automation flags disabled and use a realistic headless user agent.
4. Keep randomized waits and per-button browser isolation.
5. Do not convert it to HTML scraping.

## 4. Data and Enrichment Standards

1. Normalize price and mileage with existing helpers before insertion or analysis.
2. Validate VINs before enrichment.
3. Keep NHTSA-derived fields prefixed with `nhtsa_`.
4. Preserve SQLite table meanings:
   - `listings`: listing snapshots keyed by VIN and load date.
   - `price_history`: normalized price history rows.
   - `listing_history`: normalized listing history rows.
   - `nhtsa_enrichment`: VIN-level official metadata and safety/recall/complaint fields.
5. Prefer additive schema changes and update tests/docs with any schema change.
6. Canonical make/model use NHTSA anchors when present; title parsing verifies them and exclusively supplies canonical trim.
7. Keep EPA refresh/cache behavior conditional, atomic, offline-capable, and versioned.

## 5. Modeling and Research Standards

1. Avoid target leakage. Do not include `price`, `price_band`, future prices, or answer-derived fields in model features.
2. Preserve VIN-safe validation for current-price modeling.
3. Prefer temporal validation when sufficient dates exist.
4. Use bounded SQLite reads by default; full scans should be intentional.
5. Set random seeds for ML, sampling, and search procedures.
6. Report row counts, split strategy, metrics, caveats, and research rationale in model reports.
7. Verify new research claims against current primary sources, official documentation, or peer-reviewed papers.
8. Routine verification must not train production models; cap any explicitly needed smoke run at 5,000 rows.

## 6. Style and Hygiene Rules

1. Keep generated code professional and readable; do not use emojis.
2. Use comments only for non-obvious logic.
3. Do not create extra markdown docs unless requested; prefer updating existing project docs.
4. If you add a Python package, update `requirements.txt` in the same change.
5. Keep direct script execution intact with `if __name__ == "__main__": main()`.
6. Prefer repo-root-relative paths over machine-specific absolute paths when touching path logic.
