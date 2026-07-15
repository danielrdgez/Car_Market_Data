# AGENTS Guide

This file is the root operating guide for coding agents working in this repository. Read it before editing code, notebooks, documentation, schemas, or model workflows.

## Project Mission

This is a master's data science capstone focused on machine learning applications for the current new and used vehicle market. The repository combines web-scraped listing data, NHTSA enrichment, YouTube consumer sentiment, exploratory analysis, current-price prediction, and depreciation forecasting.

The main research goals are:

1. Measure whether safety features, safety ratings, recalls, and complaints affect price and depreciation.
2. Improve vehicle price prediction with enriched VIN-level attributes and market/listing context.
3. Forecast depreciation using cohort-level time-series features from price history.
4. Test whether NLP and aspect-based sentiment features improve predictive performance.
5. Keep the work reproducible, leakage-aware, and defensible for academic review.

## Learn These Files First

- `README.md`: public project overview, workflow, and commands.
- `PROJECT_SUMMARY.md`: technical architecture, research framing, caveats, and runbook.
- `.github/AGENTS.md`: older GitHub-scoped agent guidance that this root file consolidates.
- `.github/instructions/*.md`: targeted guidance for Python, data quality, and EDA.

Then inspect the task-relevant implementation:

- `DataPipeline/Playwright_test.py`: current Playwright global queue scraper.
- `DataPipeline/DataAquisition.py`: legacy/reference Selenium CDP scraper.
- `DataPipeline/database.py`: SQLite schema and persistence behavior.
- `DataPipeline/NHTSA_enrichment.py`: VIN decode, safety, recall, and complaint enrichment.
- `DataPipeline/DataCleaning.py`: Polars-based cleaned database builder.
- `DataPipeline/VehicleNormalization.py`: canonical identity parser and EPA catalog cache/import logic.
- `DataPipeline/SentimentAnalysis.py`: YouTube comments ingestion.
- `DataPipeline/absa_pipeline.py`: aspect-based sentiment scoring.
- `ML/Price_ML_Models.py`: current-price modeling pipeline.
- `ML/Time_Series_Price.py`: cohort depreciation forecasting.
- `tests/test_ml_upgrade.py`: regression tests for cleaning and ML safeguards.

## Repository Structure

```text
DataPipeline/       Acquisition, SQLite persistence, enrichment, cleaning, sentiment
EDA/                Python notebook, R EDA, depreciation exploration
ML/                 Current-price models, depreciation forecasts, model notebook
Utilities/          Health checks and schema migration/verification tools
tests/              Focused regression tests
streamlit_app.py    Streamlit dashboard for actuals, model metrics, predictions, forecasts
CAR_DATA_OUTPUT/    Databases, logs, CSV exports
MODELS_OUTPUT/      Model artifacts and generated reports
```

## Safe Default Commands

Use PowerShell-compatible commands on Windows.

```powershell
python Utilities\health_check.py
python Utilities\verify_schema.py
python -m unittest tests\test_ml_upgrade.py
python -m unittest tests\test_sentiment_incremental.py
python -m py_compile DataPipeline\Playwright_test.py DataPipeline\DataAquisition.py DataPipeline\DataCleaning.py DataPipeline\VehicleNormalization.py DataPipeline\NHTSA_enrichment.py DataPipeline\SentimentAnalysis.py DataPipeline\absa_pipeline.py ML\Price_ML_Models.py ML\Time_Series_Price.py Utilities\health_check.py
```

Modeling defaults are intentionally bounded. Do not switch to full-database training unless the user explicitly wants a long run.

```powershell
python ML\Price_ML_Models.py
python ML\Time_Series_Price.py
python ML\Price_ML_Models.py --task all
streamlit run streamlit_app.py
```

Use dry run before scheduled ingestion:

```powershell
run_pipeline_scheduler.bat --dry-run
```

## Scheduled Pipeline Synchronization

- `run_pipeline_scheduler.bat` is the Windows Task Scheduler entry point. Its ordered workflow is Playwright scraping, NHTSA enrichment, EPA cache refresh/validation through `VehicleNormalization.py`, then `DataCleaning.py --no-epa-refresh` so the validated catalog is imported exactly once into the cleaned database.
- When changing a DataPipeline script's scheduler-relevant behavior, inputs, outputs, dependencies, or required arguments, update both this `AGENTS.md` and `run_pipeline_scheduler.bat` in the same change. Keep the dry-run output and step counts aligned with the actual commands.

## Acquisition Rules

- Preserve API/network interception as the acquisition strategy. Do not replace `queue-results` JSON capture with HTML parsing.
- Treat `DataPipeline/Playwright_test.py` as the active scraper.
- In `Playwright_test.py`, preserve Playwright response interception for `queue-results`, global queue scheduling, bounded browser concurrency, randomized waits, and incremental persistence.
- Keep `DataAquisition.py` as the Selenium CDP reference/fallback path unless the user explicitly asks to retire or replace it.
- Keep incremental SQLite writes. Do not accumulate large scrape results in memory before persisting.
- Keep `VINCache` and database dedup behavior aligned: new rows should be inserted when VIN is new or price/mileage changed.
- Do not make scraping more aggressive without adding clear rate, retry, and stability safeguards.

## Database and Schema Rules

- `listings` is a snapshot table keyed by `(vin, loaddate)`.
- `price_history` and `listing_history` are normalized history tables.
- `nhtsa_enrichment` is VIN-level metadata.
- Keep all NHTSA-derived fields prefixed with `nhtsa_`.
- Do not drop source rows during enrichment joins unless the task explicitly asks for a scoped modeling dataset.
- Prefer additive schema changes. If a schema migration is needed, update `database.py`, `Utilities/verify_schema.py`, `Utilities/fix_database_schema.py`, tests, and documentation together.
- Do not run destructive database operations unless the user explicitly asks and a backup path is clear.

## Cleaning and Data Quality Rules

- Prefer Polars for new cleaning logic in `DataCleaning.py`.
- Keep Pandas where existing modeling, notebooks, or library APIs require it.
- Normalize price and mileage to numeric values before modeling.
- Preserve the distinction between `0`, missing, and invalid values.
- Flag or filter placeholder prices such as 0, 1, and suspiciously low values before analysis.
- Prefer robust, context-aware price outlier checks over raw global cutoffs; use make/model/year/trim cohorts when enough rows exist and preserve conservative fallbacks for sparse segments.
- Treat NHTSA make/model as canonical anchors when present and audit whether the listing title corroborates them; never overwrite those NHTSA fields with title text.
- Derive `canonical_trim` only from the listing title. EPA may standardize or validate the title result, but `nhtsa_Trim` and `nhtsa_Trim2` are comparison-only and must never supply or override canonical trim.
- Preserve `title_trim`, `trim_combined`, and `trim_source` temporarily as canonical-backed compatibility fields; new modeling must use canonical identity.
- Retain every valid NHTSA-enriched scraped make. Do not reintroduce a hard-coded make whitelist.
- Preserve `nhtsa_BasePrice` provenance; when NHTSA base price is missing, cleaned output may fill it from earliest cleaned `price_history` price, then earliest cleaned `listing_history` price.
- Validate VINs before enrichment.
- Keep cleaned output reproducible from `CAR_DATA.db` to `CAR_DATA_CLEANED.db`.
- Create or preserve indexes used by modeling and time-series queries.

## Modeling Rules

- Avoid target leakage. Do not train on `price`, `price_band`, future price fields, or any feature created from the target unless it is explicitly removed before fitting.
- Preserve VIN-safe validation. Current-price splits should avoid VIN overlap between train and test.
- Prefer time-based validation when enough dates exist; fall back to grouped validation by VIN or cohort.
- Keep bounded data loads as the default for large SQLite inputs.
- Do not run model training during implementation verification unless explicitly needed; any smoke run must use at most `--sample-size 5000`. Full runs remain explicit with `--sample-size 0`.
- Record model assumptions, row counts, split strategy, metrics, and caveats in generated reports.
- Compare readable baselines against advanced models. Do not present a complex model as better unless metrics support it.
- Segment diagnostics should include high-value vehicles, price bands, make, and model year when feasible.
- When `ML/Price_ML_Models.py` or `ML/Time_Series_Price.py` changes output schemas, metrics, artifact names, feature-importance fields, or forecast columns, update `ML/Model_Output.ipynb` in the same change so the notebook can read and summarize the corresponding outputs.
- For depreciation, preserve the cohort grain of make, model, model year, and trim proxy unless there is a research reason to change it. Keep the default output as monthly cohort-level forecasts out to five years, not VIN-level forecasts or fixed 30/60/90-day buckets. When adding or changing time-series models, preserve `cohort_future_forecasts.csv`, `cohort_backtesting_results.csv`, and `cohort_backtesting_kpis.csv` contracts so Streamlit and `ML/Model_Output.ipynb` can compare model families by cohort and horizon.

## EDA and Notebook Rules

- EDA should be hypothesis-driven and tied to the research questions.
- Prefer interactive Plotly charts in Python notebooks.
- Use deterministic samples for quick data-quality checks.
- Avoid full scans unless the user requests them or the notebook has a guarded `RUN_FULL_SCAN` flag.
- Keep notebooks reproducible with explicit paths, seeds, and assumptions.
- Keep `ML/Model_Output.ipynb` synchronized with the current ML script outputs, including current-price segment metrics and cohort forecast report fields.

## Sentiment and NLP Rules

- Never commit API keys or secrets. `YOUTUBE_API_KEY` or `GOOGLE_API_KEY` should come from environment variables, `.env`, or an explicit key file.
- Treat sentiment as optional unless the sentiment database and scored tables exist.
- Preserve comment deduplication by `comment_id`.
- Preserve video-level fetch progress in `youtube_video_fetch_state` and playlist discovery state in `youtube_playlist_fetch_state`; unseen videos should be prioritized ahead of refresh runs.
- Keep ABSA incremental by `comment_id`. Do not revert to full-table rescoring unless the user explicitly asks for a forced reprocess.
- Keep aspect labels and thresholds documented when changing `absa_pipeline.py`.
- Validate whether sentiment joins have enough support before claiming predictive lift.

## Research and Documentation Rules

- This project depends on up-to-date market methods and ML practice. When adding or changing research claims, verify against current primary sources, official documentation, or peer-reviewed papers.
- Prefer citing durable sources in generated reports or docs when the claim affects methodology.
- Keep `README.md` concise and user-facing.
- Keep `PROJECT_SUMMARY.md` as the detailed technical and capstone narrative.
- Update docs in the same change when workflows, dependencies, schemas, or model outputs change.
- Do not create extra markdown files unless the user asks. Update `README.md`, `PROJECT_SUMMARY.md`, and this file first.

## Dependency Rules

- If a new Python package is introduced, update `requirements.txt` in the same change.
- Do not silently add heavyweight ML dependencies when an existing dependency can solve the task.
- `Playwright_test.py` requires Playwright runtime setup; verify environment support before long scraping runs.
- Prefer the repo-local `.venv` when launching pipeline or scraper commands; `py -3` may resolve to a different interpreter on Windows.
- Playwright setup has two parts: the Python package from `requirements.txt` and the browser binaries from `python -m playwright install`.
- Keep direct script execution working with `if __name__ == "__main__": main()`.

## Style Rules

- Keep code professional and readable.
- Use succinct comments only where logic is not obvious.
- Avoid broad refactors unless required for the task.
- Preserve Windows-compatible commands and paths in docs, but prefer repo-root-relative paths in code.
- Keep generated markdown ASCII unless the file already needs non-ASCII.

## Known Gotchas

- `DataAquisition.py` is misspelled historically. Do not rename it casually.
- `Playwright_test.py` is the current scraper even though its filename still sounds experimental.
- `Utilities/verify_schema.py`, `Utilities/fix_database_schema.py`, `EDA/Depreciation_Analysis.py`, and `EDA/EDA_r.R` contain absolute Windows paths. Prefer improving those if the touched task involves portability.
- Git may report dubious ownership in this environment. Do not change global Git config unless the user asks.
- Some large data and model outputs may be untracked or ignored. Do not delete or overwrite them without explicit permission.
- Browser-based scraping can be slow and fragile. Prefer dry-run, health-check, and small-scope validation before long acquisition jobs.

## Definition of Done

For most changes, a good finish includes:

1. The relevant code or documentation is updated.
2. The change respects acquisition, schema, cleaning, modeling, and research constraints above.
3. Lightweight validation has been run, or the reason it could not be run is documented.
4. `README.md`, `PROJECT_SUMMARY.md`, and `AGENTS.md` are updated when workflow or architecture changes.
5. The final response clearly states what changed and what was verified.
