# Car Market Data Pipeline & Visualization

A production-grade data pipeline that scrapes car listings from AutoTempest and enriches them with NHTSA specifications, safety ratings, recalls, and complaints. Features an interactive R Shiny dashboard for data visualization and analysis. The scraper uses Selenium with stealth configuration and captures JSON via CDP performance logs to avoid brittle HTML parsing.

For the full technical documentation, workflows, and detailed troubleshooting, see `PROJECT_SUMMARY.md`.

## Quick Start

### Python Pipeline
```bash
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
```

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
│   ├── scraping_*.log
│   └── nhtsa_enrichment_*.log
├── PROJECT_SUMMARY.md
└── README.md
```

## Configuration

Update the `Config` class in `DataPipeline/DataAquisition.py` for ZIP, radius, and timing controls.

## Common Operations

### Python Pipeline
```bash
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
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
