# Car Market Data Pipeline & Visualization

A production-grade data pipeline that scrapes car listings from AutoTempest and enriches them with NHTSA specifications, safety ratings, recalls, and complaints. The project features sentiment analysis of YouTube reviews, machine learning for price prediction, and comprehensive exploratory data analysis.

For the full technical documentation, workflows, and detailed troubleshooting, see `PROJECT_SUMMARY.md`.

## Quick Start

### Python Pipeline
```bash
pip install -r requirements.txt
python Utilities\health_check.py
python DataPipeline\DataAquisition.py
python DataPipeline\NHTSA_enrichment.py
python DataPipeline\DataCleaning.py

# Optional: Sentiment Analysis
python DataPipeline\SentimentAnalysis.py --video-id [VIDEO_ID]

# Optional: Machine Learning
python ML\Price_ML_Models.py
```

### Notebook Troubleshooting

If you see `Mime type rendering requires nbformat>=4.2.0 but it is not installed`, your notebook kernel is using a different interpreter than the project environment.

```bash
pip install -r requirements.txt
python -m ipykernel install --user --name car-market-data --display-name "Python (car-market-data)"
```

Then select `Python (car-market-data)` as the kernel for `EDA/EDA_notebook.ipynb`.

### Data Analysis & Visualization

- **Python (Plotly):** Use `EDA/EDA_notebook.ipynb` for interactive exploratory data analysis.
- **Python (Depreciation):** Use `EDA/Depreciation_Analysis.py` to calculate and visualize vehicle depreciation metrics.
- **R (ggplot2):** Use `EDA/EDA_r.R` for statistical analysis and visualization in R.

## Repository Layout

```
Car-Price-Data-Visualization-Learning/
├── DataPipeline/
│   ├── DataAquisition.py
│   ├── database.py
│   ├── DataCleaning.py
│   ├── NHTSA_enrichment.py
│   ├── SentimentAnalysis.py
│   └── migrate_to_db.py
├── EDA/
│   ├── EDA_notebook.ipynb    # Main Python EDA
│   ├── Depreciation_Analysis.py
│   └── EDA_r.R               # R Analysis script
├── ML/
│   ├── Price_ML_Models.py    # Training pipelines
│   └── Model_Output.ipynb    # Evaluation
├── MODELS_OUTPUT/            # Trained model binaries (.joblib)
├── Utilities/
│   ├── health_check.py
│   ├── verify_schema.py
│   └── fix_database_schema.py
├── CAR_DATA_OUTPUT/
│   ├── CAR_DATA.db           # Raw scraped data
│   ├── CAR_DATA_CLEANED.db   # Cleaned & joined data
│   └── *.log                 # Operation logs
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
python DataPipeline\SentimentAnalysis.py --video-id [ID]
python ML\Price_ML_Models.py
python Utilities\verify_schema.py
python Utilities\fix_database_schema.py
```

### R Analysis

```r
# Load data and generate plots
source("EDA/EDA_r.R")
```

## Notes

- `PROJECT_SUMMARY.md` consolidates the technical documentation for the entire pipeline.
- Sentiment analysis requires a YouTube API key in a `.env` file or environment variable.
- Utility scripts were moved to `Utilities/` to keep `DataPipeline/` focused on core pipeline code.
