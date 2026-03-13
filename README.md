# Car Market Data Pipeline & Visualization

A production-grade data pipeline that scrapes car listings from AutoTempest and enriches them with NHTSA specifications, safety ratings, recalls, and complaints. The scraper uses per-button parallel browser instances with Selenium stealth configuration, capturing JSON via CDP performance log interception. Each source button (autotempest, hemmings, cars, etc.) runs in its own dedicated thread with an isolated browser, clicking until true exhaustion. A thread-safe VIN cache and locked database writes ensure data integrity across concurrent workers.

For the full technical documentation, workflows, and detailed troubleshooting, see `PROJECT_SUMMARY.md`.

## Quick Start

### Python Pipeline
```bash
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py
```

### Notebook Troubleshooting

If you see `Mime type rendering requires nbformat>=4.2.0 but it is not installed`, your notebook kernel is using a different interpreter than the project environment.

```bash
pip install -r requirements.txt
python -m ipykernel install --user --name car-market-data --display-name "Python (car-market-data)"
```

Then select `Python (car-market-data)` as the kernel for `EDA/EDA_notebook.ipynb`.

### Shiny Dashboard (Visualization)

```r
# In R or RStudio
source("Visualizations/requirements.R")  # First time only
shiny::runApp("EDA")
```

## Repository Layout

```
Car-Price-Data-Visualization-Learning/
├── DataPipeline/
│   ├── DataAquisition.py
│   ├── database.py
│   ├── DataCleaning.py
│   ├── NHTSA_enrichment.py
│   └── migrate_to_db.py
├── Visualizations/
│   ├── app.R                 # Shiny dashboard application
│   ├── requirements.R        # R package dependencies
│   ├── README.md            # Dashboard documentation
│   └── QUICKSTART.md        # Quick start guide
├── Utilities/
│   ├── health_check.py
│   ├── verify_schema.py
│   └── fix_database_schema.py
├── CAR_DATA_OUTPUT/
│   ├── CAR_DATA.db
│   ├── CAR_DATA_CLEANED.db
│   ├── scraping_*.log
│   ├── nhtsa_enrichment_*.log
│   └── cleaning_*.log
├── PROJECT_SUMMARY.md
└── README.md
```

## Configuration

Update the `ScrapingConfig` dataclass in `DataPipeline/DataAquisition.py` for ZIP, makes list, concurrent button workers, exhaustion thresholds, and timing controls.

## Common Operations

### Python Pipeline
```bash
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py
python Utilities\verify_schema.py
python Utilities\fix_database_schema.py
```

### R Shiny Dashboard

```r
# Install dependencies (first time only)
source("Visualizations/requirements.R")

# Run the dashboard
shiny::runApp("EDA")
```

See `Visualizations/QUICKSTART.md` for detailed dashboard instructions.

## Notes

- `PROJECT_SUMMARY.md` consolidates the previous documentation set.
- Utility scripts were moved to `Utilities/` to keep `DataPipeline/` focused on core pipeline code.
