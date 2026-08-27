---
applyTo: ["DataPipeline/DataCleaning.py", "DataPipeline/VehicleNormalization.py", "DataPipeline/NHTSA_enrichment.py"]
---

# Data Quality and Enrichment Standards

## Cleaning Rules (`DataPipeline/DataCleaning.py`)
- Prefer `polars` as the default dataframe engine for new cleaning logic.
- Keep behavior stable when touching existing `pandas` code; migrate incrementally.
- Price cleaning must normalize to numeric values (strip `$`, `,`, whitespace; coerce invalid strings).
- Mileage cleaning must preserve the difference between true zero mileage, missing mileage, and invalid strings.
- Standardize make/model casing consistently.
- Build the cleaned modeling database from `CAR_DATA_OUTPUT/CAR_DATA.db` into `CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db`.
- Preserve modeling indexes used by `ML/Price_ML_Models.py` and `ML/Time_Series_Price.py`.
- Treat suspicious placeholder prices such as 0, 1, and very low "call for price" values as data-quality issues before analysis.
- Use NHTSA make/model as canonical anchors when present; parse the title to verify agreement and never overwrite those NHTSA fields.
- Derive `canonical_trim` only from listing-title text. Use EPA to standardize/validate it, retain unmatched normalized remainders, and flag empty results as `UNKNOWN_TRIM`.
- Preserve `nhtsa_Trim` and `nhtsa_Trim2` unchanged as comparison fields. They cannot contribute text to canonical trim.
- Retain all valid NHTSA-enriched makes; do not filter against a hard-coded make list.
- Refresh EPA data conditionally with atomic replacement and validated-cache fallback; record source/hash/coverage metadata and import the complete catalog into the cleaned database.

## NHTSA Enrichment Rules (`DataPipeline/NHTSA_enrichment.py`)
- Validate VINs with `_is_valid_vin` before API calls.
- Respect API rate limiting and keep retry/backoff behavior conservative.
- Keep merge semantics as left-join style behavior to avoid dropping source rows.
- Prefix all NHTSA-derived fields with `nhtsa_` to avoid naming collisions.
- Cache repeated make/model/year lookups where possible to reduce redundant safety, recall, and complaint calls.
- Store normalized NHTSA history in `CAR_DATA_OUTPUT/CAR_DATA_NHTSA.db`; treat `nhtsa_enrichment` in `CAR_DATA.db` as a compatibility projection.
- Store every source field in typed columns, one dynamically widened vPIC row per decode, or normalized field/value tables for sparse extras, while excluding full-response JSON, request payloads, per-record JSON copies, and raw bulk-row blobs. Preserve response messages, error codes, HTTP status, request timestamps, and source hashes.
- Include model-year hints in vPIC batch payloads and never exceed 50 VINs per request.
- Query Safety Ratings variants first, then retrieve details for every returned `VehicleId`.
- Preserve every recall and complaint row and all source fields; do not confuse make/model/year counts with VIN-specific reliability rates.
- Use NHTSA make/model/year first and listing values as field-level fallbacks, recording source, confidence, and conflicts.
- Refresh old VINs through resumable cache-aware runs instead of processing only VINs absent from the compatibility table.
- Before the first run that alters an existing primary schema, create a new backup with --backup-path; do not overwrite prior backups.
- Use --backfill-legacy for a deliberate all-historical-VIN refresh that bypasses the NHTSA response freshness cache.

## Modeling Data Standards
- Avoid target leakage in cleaned or engineered datasets.
- Do not include target-derived fields such as `price_band` in model features.
- Preserve VIN-safe train/test splitting and temporal validation where possible.
- Record row counts, dropped-row reasons, and validation caveats in reports or notebooks.
- Exclude raw NHTSA/legacy trims, identity provenance, confidence, agreement flags, and EPA IDs from predictive features.

## Style and Dependency Hygiene
- Keep generated code and markdown professional and emoji-free.
- Use only minimal comments for non-obvious logic.
- When introducing new packages, update `requirements.txt` in the same change.
- When NHTSA behavior, schema, scheduler commands, or lineage changes, update all project Markdown and agent guidance files in the same change.
