---
applyTo: ["DataPipeline/DataCleaning.py", "DataPipeline/NHTSA_enrichment.py"]
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

## NHTSA Enrichment Rules (`DataPipeline/NHTSA_enrichment.py`)
- Validate VINs with `_is_valid_vin` before API calls.
- Respect API rate limiting and keep retry/backoff behavior conservative.
- Keep merge semantics as left-join style behavior to avoid dropping source rows.
- Prefix all NHTSA-derived fields with `nhtsa_` to avoid naming collisions.
- Cache repeated make/model/year lookups where possible to reduce redundant safety, recall, and complaint calls.

## Modeling Data Standards
- Avoid target leakage in cleaned or engineered datasets.
- Do not include target-derived fields such as `price_band` in model features.
- Preserve VIN-safe train/test splitting and temporal validation where possible.
- Record row counts, dropped-row reasons, and validation caveats in reports or notebooks.

## Style and Dependency Hygiene
- Keep generated code and markdown professional and emoji-free.
- Use only minimal comments for non-obvious logic.
- When introducing new packages, update `requirements.txt` in the same change.
