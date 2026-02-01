# Project Roadmap: Car Market Analysis

## Phase 1: Data Enrichment (NHTSA vPIC API)
- [ ] Develop script to query NHTSA vPIC API (`https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/{VIN}?format=json`).
- [ ] Implement rate limiting and batch processing for API requests.
- [ ] Extract critical vehicle specifications:
    - Engine (Cylinders, Displacement, Configuration)
    - Transmission type
    - Drive type (FWD/RWD/AWD)
    - Fuel type
    - Base model and trim level
    - Manufacturing plant and country
- [ ] Handle API timeout errors and invalid VIN responses.

## Phase 2: Data Cleaning & Transformation
- [ ] Merge NHTSA specification data with existing AutoTempest scraper data on VIN.
- [ ] Standardize field formats:
    - Convert Price and Mileage to integers.
    - Normalize text fields (trim, lowercase/uppercase consistency).
- [ ] Address missing values:
    - Define logic for records with valid prices but invalid VINs.
    - Impute missing trim levels where possible.
- [ ] Deduplicate records based on VIN and scrape date.

## Phase 3: Storage & Analysis
- [ ] Design final schema for the enriched dataset.
- [ ] Export clean data to CSV/SQL for visualization.
- [ ] Create data dictionary defining all variable units and sources.

## Backlog / Future Tasks
- [ ]
- [ ]