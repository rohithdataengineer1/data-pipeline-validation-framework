# Data Pipeline Testing & Validation Framework

A production-ready ETL pipeline with comprehensive data quality validation checks. This framework prevents silent data corruption by implementing automated validation at each stage of the data pipeline.

## 🎯 Problem Statement

Data pipelines often fail silently - bad data reaches downstream systems without detection, leading to:
- Incorrect business decisions based on flawed analytics
- Data quality issues discovered too late in production
- Time-consuming manual data quality checks
- Lack of trust in data across teams

This framework solves these problems by implementing automated validation checks that catch data quality issues before they propagate.

## 🏗️ Architecture
```
Raw Data (CSV) 
    ↓
[EXTRACT] - Read and validate file existence
    ↓
[TRANSFORM] - Clean, type convert, calculate
    ↓
[VALIDATE] - 8 automated quality checks
    ↓
[LOAD] - Save to database (only if validation passes)
    ↓
Clean Data (SQLite)
```

## ✨ Features

### Data Validation Checks
1. **Schema Validation** - Ensures all expected columns are present
2. **Row Count Check** - Verifies no data loss during transformation
3. **Null Value Check** - Detects missing values in critical columns
4. **Data Type Check** - Confirms correct data types for each column
5. **Duplicate Check** - Identifies duplicate records by key column
6. **Range Validation** - Checks values fall within expected ranges
7. **Transformation Accuracy** - Verifies calculated fields are correct
8. **Source vs Target Comparison** - Ensures data integrity across pipeline

### Additional Features
- Comprehensive error handling and logging
- Detailed validation reports
- Database verification after load
- Modular, reusable code structure
- Easy to extend with new validation rules

## 🛠️ Tech Stack

- **Python 3.11** - Core programming language
- **Pandas** - Data manipulation and transformation
- **SQLite** - Lightweight database for data storage
- **Git** - Version control

## 📁 Project Structure
```
data-validation-project/
├── data/
│   ├── raw/              # Source CSV files
│   ├── processed/        # Intermediate cleaned data
│   └── warehouse/        # Final SQLite database
├── src/
│   ├── extract.py        # Data extraction logic
│   ├── transform.py      # Data transformation logic
│   ├── load.py           # Database loading logic
│   ├── validate.py       # Validation framework
│   └── pipeline.py       # Master orchestration script
├── tests/                # Unit tests (future enhancement)
├── reports/              # Validation reports (future enhancement)
└── README.md
```

## 🚀 Quick Start

### Prerequisites
```bash
python >= 3.8
pip
```

### Installation
```bash
# Clone the repository
git clone https://github.com/rohithdataengineer1/data-pipeline-validation-framework.git
cd data-pipeline-validation-framework

# Install dependencies
pip install -r requirements.txt
```

### Run the Pipeline
```bash
# Run the complete ETL pipeline with validation
python src/pipeline.py
```

### Run Individual Components
```bash
# Extract only
python src/extract.py

# Transform only
python src/transform.py

# Validate only
python src/validate.py

# Load only
python src/load.py
```

## 📊 Sample Output
```
============================================================
 DATA PIPELINE WITH VALIDATION FRAMEWORK
============================================================

[STEP 1] EXTRACT
✓ Extracted 10 rows

[STEP 2] TRANSFORM
✓ Transformation complete! 10 rows ready

[STEP 3] VALIDATE
✓ Schema Validation: PASSED
✓ Row Count Check: PASSED  
✓ Null Check: PASSED
✓ Data Type Check: PASSED
✓ Duplicate Check: PASSED
✓ Range Check (price): PASSED
✓ Range Check (quantity): PASSED
✓ Transformation Accuracy: PASSED

Total Checks: 8
Passed: 8
Failed: 0

🎉 ALL VALIDATIONS PASSED! 🎉

[STEP 4] LOAD
✓ Loaded 10 rows to database

============================================================
✓ PIPELINE COMPLETED SUCCESSFULLY!
============================================================
```

## 🎓 Key Learnings

- Implementing production-grade data validation patterns
- Building modular, maintainable ETL pipelines
- Error handling and defensive programming in data engineering
- SQL database operations and verification
- Data quality best practices

## 🔮 Future Enhancements

- [ ] Add pytest unit tests for all components
- [ ] Generate HTML validation reports
- [ ] Implement email alerts for validation failures
- [ ] Add support for multiple data sources (APIs, databases)
- [ ] Integrate with Apache Airflow for scheduling
- [ ] Add Great Expectations for advanced validation
- [ ] Implement data lineage tracking
- [ ] Add performance metrics and monitoring

## 📝 License

This project is open source and available under the MIT License.

## 🤝 Connect

- **LinkedIn:** [Rohith Vyda](https://www.linkedin.com/in/rohith-vyda)
- **GitHub:** [@rohithdataengineer1](https://github.com/rohithdataengineer1)

---

*Built with focus on data quality and reliability*