# Data Pipeline Testing & Validation Framework

## Project Goal
Learn ETL by building a pipeline that validates data quality at each step.

## What This Project Does
- Extracts data from CSV files
- Transforms and cleans the data
- Validates data quality with automated checks
- Loads clean data into SQLite database

## Tech Stack
- Python
- Pandas (data transformation)
- SQLite (database)
- Pytest (testing)

## Project Structure
- `data/raw/` - Original CSV files
- `data/processed/` - Cleaned data
- `data/warehouse/` - SQLite database
- `src/` - Python scripts
- `tests/` - Test files
- `reports/` - Validation reports