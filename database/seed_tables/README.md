# Database Seed Data

CSV files and scripts for populating the COEQWAL PostgreSQL database with foundational data.

## 📁 Directory Organization

```
seed_tables/
├── 00_versioning/          # Core versioning system (✅ loaded)
│   ├── developer.csv       # System and user accounts (2 records)
│   ├── version_family.csv  # Version domains (13 families)
│   ├── version.csv         # Version instances (13 versions)
│   └── domain_family_map.csv # Table-to-family mappings (35 mappings)
├── 01_infrastructure/      # Reference data (✅ loaded)
│   ├── hydrologic_region.csv # Water basins (5 regions)
│   ├── unit.csv            # Measurement units (5 units)
│   ├── source.csv          # Data sources (8 sources)
│   ├── spatial_scale.csv   # Geographic scales (11 scales)
│   └── temporal_scale.csv  # Time scales (8 scales)
├── 02_entity_system/       # Entity types (✅ loaded)
│   ├── calsim_entity_type.csv
│   ├── calsim_schematic_type.csv (2 records)
│   ├── network_*_type.csv  # Network classification
│   └── variable_type.csv   (3 records)
├── 04_calsim_data/        # Major CalSim data (✅ loaded)
│   ├── network_topology.csv    # 3,518 network elements
│   ├── network_node.csv        # 7,742 nodes
│   ├── network_arc.csv         # 6,965 arcs
│   ├── network_gis.csv         # 2,463 spatial features
│   ├── reservoir_entity.csv    # 63 reservoirs
│   ├── du_agriculture_entity.csv # 144 ag demand units
│   ├── du_urban_entity.csv     # 106 urban demand units
│   └── du_refuge_entity.csv    # 18 wildlife refuges
├── 05_themes_scenarios/    # Research framework
├── 06_assumptions_operations/
└── 07_hydroclimate/       
```

## 🔄 Loading status

### ✅ Successfully Loaded Categories:
1. **00_versioning/** - Core versioning system (13 families, 35 mappings)
2. **01_infrastructure/** - Reference data (5 regions, 8 sources, scales)
3. **02_entity_system/** - Entity types and classifications
4. **04_calsim_data/** - Major network and entity data (14,000+ records)

### 🔄 Partially Loaded:
5. **05_themes_scenarios/** - Research framework
6. **06_assumptions_operations/** - Policy framework  
7. **07_hydroclimate/** - Climate data

## 🚀 Database Management

### Audit Your Database
```bash
# Run comprehensive database audit via Lambda
export DATABASE_URL="postgresql://username:password@your-rds-endpoint:5432/coeqwal_scenario"
aws lambda invoke --function-name coeqwal-database-audit response.json
cat response.json

# Download detailed reports
aws s3 cp s3://coeqwal-model-run/database_audits/audit_YYYYMMDD_HHMMSS.json .
aws s3 cp s3://coeqwal-model-run/database_audits/tables_summary_YYYYMMDD_HHMMSS.csv .
```




### Network topology system
- **3,518 network elements** in core topology
- **7,742 nodes** with spatial coordinates
- **6,965 arcs** with connectivity data
- **2,463 GIS features** with PostGIS geometry

### Entity system  
- **63 reservoirs** with capacity and operational data
- **144 agricultural demand units** with acreage and diversions
- **106 urban demand units** with community data
- **18 wildlife refuges** with habitat information

### Versioning & audit
- **13 version families** covering all data domains
- **developers** tracked
- **24 tables** with full audit trails
