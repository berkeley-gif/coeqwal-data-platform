# 06_assumptions_operations

This directory contains policy assumptions and operational rules that define how the water management system behaves under different scenarios.

## 📋 Assumptions (Version Family 3)

Policy assumptions that drive scenario behavior:

### Core tables
- `assumption_category.csv` - Categories: TUCP/TUCO, land use, groundwater restrictions, SLR, etc.
- `assumption_definition.csv` - Specific assumption definitions with parameters

### Parameter tables  
- `assumption_param_tucp_tuco.csv` - TUCP/TUCO parameters by region/season
- `assumption_param_land_use.csv` - Crop types, amounts, regions
- `assumption_param_sgma.csv` - SGMA groundwater restrictions
- `assumption_param_slr.csv` - Sea level rise projections
- `assumption_param_gwmodel.csv` - Groundwater model parameters
- `assumption_param_bioops.csv` - Biological operations parameters
- `assumption_param_kv.csv` - Key-value pairs for other assumption types

## ⚙️ Operations (Version Family 4)

Operational policies and rules that govern system behavior:

### Core tables
- `operation_category.csv` - Categories: infrastructure, regulatory, priority allocation, minimum flow, etc.
- `operation_definition.csv` - Specific operational policy definitions

### Parameter tables
- `operation_param_priority_allocation.csv` - Water allocation priorities by region/season
- `operation_param_minimum_flow.csv` - Minimum instream flow requirements
- `operation_param_infrastructure.csv` - Infrastructure operational rules
- `operation_param_regulatory.csv` - Regulatory compliance rules  
- `operation_param_carryover.csv` - Reservoir carryover storage rules
- `operation_param_kv.csv` - Key-value pairs for other operation types

## 🔗 Links to themes & scenarios

These assumptions and operations are linked to themes and scenarios via:
- `theme_key_assumption_link` - Which assumptions are key for each theme
- `theme_key_operation_link` - Which operations are key for each theme  
- `scenario_key_assumption_link` - Which assumptions apply to each scenario
- `scenario_key_operation_link` - Which operations apply to each scenario
