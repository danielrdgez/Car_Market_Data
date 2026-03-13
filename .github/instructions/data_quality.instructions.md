---
applyTo: ["DataPipeline/DataCleaning.py", "DataPipeline/NHTSA_enrichment.py"]
---

# Data Quality and Enrichment Standards

## Cleaning Rules (`DataPipeline/DataCleaning.py`)
- Prefer `polars` as the default dataframe engine for new cleaning logic.
- Keep behavior stable when touching existing `pandas` code; migrate incrementally.
- Price cleaning must normalize to numeric values (strip `$`, `,`, whitespace; coerce invalid strings).
- Standardize make/model casing consistently and process the latest data file by creation time.

## NHTSA Enrichment Rules (`DataPipeline/NHTSA_enrichment.py`)
- Validate VINs with `_is_valid_vin` before API calls.
- Respect API rate limiting and keep retry/backoff behavior conservative.
- Keep merge semantics as left-join style behavior to avoid dropping source rows.
- Prefix all NHTSA-derived fields with `nhtsa_` to avoid naming collisions.

## Style and Dependency Hygiene
- Keep generated code and markdown professional and emoji-free.
- Use only minimal comments for non-obvious logic.
- When introducing new packages, update `requirements.txt` in the same change.
