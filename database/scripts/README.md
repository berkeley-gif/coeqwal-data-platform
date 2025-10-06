# Database scripts

This directory contains scripts for database management, data processing, and analysis.

## Directory structure

```
database/scripts/
├── data_processing/          # Data integration and seed file generation
│   └── create_network_seeds_from_sources.py
├── sql/                     # SQL scripts for database operations
│   └── (SQL scripts)
└── utils/                   # Database utilities (Lambda functions, etc.)
    └── db_audit_lambda/
```

## Execution environments

### **Local development**
```bash
# Requirements: Python 3.9+, pandas
cd /Users/jfantauzza/coeqwal-backend
python3 database/scripts/data_processing/create_network_seeds_from_sources.py
```

### **AWS Cloud9 (Database operations)**
```bash
# For database loading and SQL operations
# Cloud9 has direct access to RDS PostgreSQL
psql postgresql://postgres:password@coeqwal-scenario-database-1.clai4yqcyzxh.us-west-2.rds.amazonaws.com:5432/coeqwal_scenario
```

### **AWS Lambda (Production operations)**
```bash
# For automated database audits and operations
aws lambda invoke --function-name coeqwal-database-audit response.json --region us-west-2
```

## Data processing scripts

### **create_network_seeds_from_sources.py**
**Purpose**: Generate normalized network seed files from authoritative geopackage sources
**Input**: 
- `CalSim_arcs_geopackage.csv` (2,118 arcs)
- `CalSim_nodes_geopackage.csv` (1,400 nodes)
-`CS3_NetworkSchematic_Integrated_11.28.23.xml`
**Output**:
- `network_topology.csv` (connectivity master)
- `network_arc.csv` (arc details)
- `network_node.csv` (node details)
- `network_gis.csv` (PostGIS spatial data)

**Environment**: Local (requires pandas)
**Usage**:
```bash
cd /Users/jfantauzza/coeqwal-backend
python3 database/scripts/data_processing/create_network_seeds_from_sources.py
```

### **create_type_mappings.py**
**Purpose**: Analyze type mappings between geopackage data and database types
**Environment**: Local
**Usage**: Analysis tool to verify data compatibility

## Environments

1. **Data processing**: Run locally (needs pandas)
2. **Database operations**: Use AWS Cloud9 (direct RDS access)
3. **Production operations**: Use AWS Lambda (automated, scalable)
