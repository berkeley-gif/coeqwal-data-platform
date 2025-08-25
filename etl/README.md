# ETL (Extract, Transform, Load) Framework

This directory contains all data processing pipelines for the CalSim backend system.

## 📁 Directory structure

```
etl/
├── pipelines/              # High-level ETL workflow definitions
│   ├── dss_processing/     # DSS file processing pipelines
│   ├── gis_integration/    # GIS data processing
│   ├── variable_mapping/   # CalSim variable normalization
│   └── statistics_compute/ # Summary statistics computation
├── transformers/          # Data transformation modules
│   ├── channel_transform.py
│   ├── inflow_transform.py
│   ├── reservoir_transform.py
│   └── gis_transform.py
├── loaders/               # Database loading utilities
│   ├── bulk_loader.py
│   ├── incremental_loader.py
│   └── entity_loader.py
└── validators/            # Data quality and validation
    ├── schema_validator.py
    ├── business_rules.py
    └── data_quality.py
```

## 🔄 ETL pipelines

### 1. DSS processing pipeline
**Purpose**: Process CalSim DSS files and extract variable data
**Input**: CalSim model run
**Output**: Output variable csv, SV csv

```python
# Example usage
from etl.pipelines.dss_processing import DSSPipeline

pipeline = DSSPipeline(
    input_dir="data/raw/dss_files/",
    output_dir="data/processed/csv_files/"
)
pipeline.run()
```

### 2. GIS integration pipeline  
**Purpose**: Process spatial data and integrate with CalSim variables
**Input**: GIS shapefiles from CalSim geopackage
**Output**: Variable data with spatial attributes

### 3. Variable mapping pipeline
**Purpose**: Normalize and map CalSim variables across sources
**Input**: Variable lists
**Output**: Unified variable tables with source tracking

### 4. Statistics computation pipeline
**Purpose**: Compute summary statistics for scenarios
**Input**: Time series data, variable values
**Output**: Statistical summaries by scenario/theme

## 🛠️ Key components

### Transformers
Data transformation modules that handle:
- **Data Cleaning**: Remove duplicates, handle missing values
- **Normalization**: Standardize units, naming conventions
- **Validation**: Ensure data quality and business rules
- **Enrichment**: Add calculated fields, lookups

### Loaders
Database loading utilities that handle:
- **Bulk Loading**: Initial data loads from CSV files
- **Incremental Loading**: Updates and new data
- **Relationship Management**: Foreign key handling
- **Error Recovery**: Transaction management

### Validators
Data quality framework that ensures:
- **Schema Compliance**: Correct data types and formats
- **Business Rules**: CalSim-specific validation rules
- **Data Quality**: Completeness, accuracy, consistency
- **Referential Integrity**: Valid foreign key relationships

## 🚀 Running ETL processes

### Command line interface
```bash
# Run full ETL pipeline
python -m etl.pipelines.main --full

# Run specific pipeline
python -m etl.pipelines.dss_processing --input data/raw/sv_file.dss

# Validate data quality
python -m etl.validators.main --check-all

# Load data to database
python -m etl.loaders.bulk_loader --table channel_variables
```

### Programmatic usage
```python
from etl.pipelines import DSSPipeline, GISPipeline
from etl.loaders import BulkLoader

# Process DSS data
dss_pipeline = DSSPipeline()
processed_data = dss_pipeline.transform("path/to/file.dss")

# Load to database
loader = BulkLoader()
loader.load_table("channel_variables", processed_data)
```

## 📊 Integration with existing DSS export repo

```python
# etl/pipelines/dss_processing.py
import subprocess
from pathlib import Path

class DSSPipeline:
    def __init__(self, dss_repo_path: str):
        self.dss_repo_path = Path(dss_repo_path)
    
    def extract_dss(self, dss_file: str) -> str:
        """Use existing DSS-to-CSV tools"""
        cmd = f"docker run -v {dss_file}:/input dss-converter"
        result = subprocess.run(cmd, shell=True, capture_output=True)
        return result.stdout.decode()
    
    def transform(self, csv_data: str) -> dict:
        """Apply CalSim-specific transformations"""
        # Your transformation logic here
        pass
```

## 🔍 Monitoring and logging

All ETL processes include:
- **Progress tracking**: Real-time pipeline status
- **Error handling**: Graceful failure recovery
- **Data lineage**: Track data flow and transformations
- **Performance metrics**: Processing time and throughput

## 🧪 Testing

- **Data quality tests**: Validation rule testing
- **Performance tests**: Load and stress testing 