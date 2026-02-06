---

### 3. Data Quality & Enrichment Instructions
**File path:** `.github/instructions/data_quality.instructions.md`
**Purpose:** Rules for `DataCleaning.py` and `NHTSA_enrichment.py` to ensure clean data merging.

```markdown
---
applyTo: ["DataCleaning.py", "NHTSA_enrichment.py"]
---

# Data Quality & Enrichment Standards

## 1. Cleaning Rules (`DataCleaning.py`)
- **Price Cleaning:** Remove '$', ',', and whitespace. Convert 'Inquire', 'Call', or '0' to `pd.NA`.
- **String Standardization:** Title case all Makes/Models (e.g., "bmw" -> "BMW").
- **File Selection:** Always process the *latest* CSV by creation time (`os.path.getctime`) to avoid re-cleaning old data.

## 2. NHTSA Enrichment Rules (`NHTSA_enrichment.py`)
- **Rate Limiting:** Respect the NHTSA API limit. Enforce a minimum 0.5s delay between requests.
- **VIN Validation:** Check `_is_valid_vin(vin)` (length check + simple regex) before making an API call.
- **Merging:** Use `how='left'` when merging enrichment data to ensure we never lose the original scraping rows, even if enrichment fails.
- **Naming Convention:** Prefix all NHTSA-derived columns with `nhtsa_` (e.g., `nhtsa_EngineHP`) to avoid collision with AutoTempest data.