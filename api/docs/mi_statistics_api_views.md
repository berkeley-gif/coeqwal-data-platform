# M&I Statistics API - Front-End Views

## Overview

This document describes the API endpoints for M&I (Municipal & Industrial) water delivery and shortage statistics. Three levels of data granularity are available:

| Level | Entity | Count | Description |
|-------|--------|-------|-------------|
| 1 | **CWS Aggregates** | 4 | High-level project totals (SWP M&I, CVP North, CVP South, MWD) |
| 2 | **M&I Contractors** | 30 | SWP water agency contractors |
| 3 | **Demand Units** | 125 | Geographic urban demand units |

---

## Views Required

| View | Unit | Time Periods | Summary Stats | Entity Level | Description |
|------|------|--------------|---------------|--------------|-------------|
| M&I Surface Water Deliveries | acre-feet (TAF) | Monthly, Annually | Annual avg, CV, dry-year avg | Demand Unit, Contractor, Aggregate | Volumetric surface water deliveries |
| M&I Deliveries as % of Demand | percent | Monthly, Annually | Annual avg, CV, dry-year avg | Demand Unit, Contractor, Aggregate | Percent of total M&I demand delivered |
| Absolute M&I Supply Shortage | acre-feet (TAF) | Monthly, Annually | Annual avg, CV, dry-year avg | Demand Unit, Contractor, Aggregate | Reduction in supply vs. demands |

---

## API Endpoints

### Base Path: `/api/statistics`

---

### 1. CWS Aggregate Endpoints (Project-Level Totals)

**Status: Data loaded, endpoints need to be created**

#### List Aggregates
```
GET /api/statistics/cws-aggregates
```
Returns available CWS aggregate entities.

**Response:**
```json
{
  "aggregates": [
    {"short_code": "swp_total", "label": "SWP Total M&I"},
    {"short_code": "cvp_nod", "label": "CVP North"},
    {"short_code": "cvp_sod", "label": "CVP South"},
    {"short_code": "mwd", "label": "Metropolitan Water District"}
  ]
}
```

#### Monthly Statistics
```
GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/monthly
GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/monthly?aggregate=swp_total
GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/monthly?aggregate=cvp_nod,cvp_sod
```

**Response:**
```json
{
  "scenario_id": "s0020",
  "aggregates": {
    "swp_total": {
      "label": "SWP Total M&I",
      "monthly_delivery": {
        "1": {
          "avg_taf": 125.5,
          "cv": 0.35,
          "q0": 45.2, "q10": 78.3, "q30": 95.1, "q50": 118.2, "q70": 145.3, "q90": 180.1, "q100": 220.5,
          "exc_p5": 195.2, "exc_p10": 175.3, "exc_p25": 150.1, "exc_p50": 118.2, "exc_p75": 88.5, "exc_p90": 65.2, "exc_p95": 52.1
        },
        "2": { ... },
        ...
        "12": { ... }
      },
      "monthly_shortage": {
        "1": {
          "avg_taf": 12.5,
          "cv": 1.2,
          "frequency_pct": 35.5,
          "q0": 0.0, "q10": 0.0, "q30": 0.0, "q50": 5.2, "q70": 15.3, "q90": 35.1, "q100": 85.5
        },
        ...
      }
    },
    "cvp_nod": { ... }
  }
}
```

#### Period Summary
```
GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/period-summary
GET /api/statistics/scenarios/{scenario_id}/cws-aggregates/period-summary?aggregate=swp_total
```

**Response:**
```json
{
  "scenario_id": "s0020",
  "aggregates": {
    "swp_total": {
      "label": "SWP Total M&I",
      "simulation_start_year": 1922,
      "simulation_end_year": 2021,
      "total_years": 100,
      "annual_delivery_avg_taf": 1506.5,
      "annual_delivery_cv": 0.28,
      "annual_delivery_min_taf": 850.2,
      "annual_delivery_max_taf": 2150.3,
      "delivery_exceedance": {
        "p5": 2050.5, "p10": 1950.2, "p25": 1750.1, "p50": 1506.5, "p75": 1250.3, "p90": 1050.1, "p95": 920.5
      },
      "annual_shortage_avg_taf": 150.5,
      "shortage_years_count": 45,
      "shortage_frequency_pct": 45.0,
      "shortage_exceedance": {
        "p5": 450.2, "p10": 380.5, "p25": 250.1, "p50": 120.5, "p75": 35.2, "p90": 0.0, "p95": 0.0
      },
      "reliability_pct": 90.0,
      "avg_pct_allocation_met": 91.5
    }
  }
}
```

---

### 2. M&I Contractor Endpoints (SWP Agency Level)

**Status: Entity data loaded, statistics ETL pending**

#### List Contractors
```
GET /api/statistics/mi-contractors
GET /api/statistics/mi-contractors?group=swp_mi
```

**Response:**
```json
{
  "contractors": [
    {"short_code": "ACFC", "name": "Alameda County FC&WCD-Zone 7", "contractor_type": "MI"},
    {"short_code": "ACWD", "name": "Alameda County WD", "contractor_type": "MI"},
    {"short_code": "MWDSC", "name": "Metropolitan WDSC", "contractor_type": "MWD"},
    ...
  ]
}
```

#### Monthly Delivery Statistics
```
GET /api/statistics/scenarios/{scenario_id}/mi-contractors/delivery-monthly
GET /api/statistics/scenarios/{scenario_id}/mi-contractors/delivery-monthly?contractor=MWDSC,ACWD
```

#### Monthly Shortage Statistics
```
GET /api/statistics/scenarios/{scenario_id}/mi-contractors/shortage-monthly
```

#### Period Summary
```
GET /api/statistics/scenarios/{scenario_id}/mi-contractors/period-summary
```

---

### 3. Urban Demand Unit Endpoints (Geographic Level)

**Status: Entity data loaded, statistics ETL pending**

#### List Demand Units
```
GET /api/statistics/demand-units
GET /api/statistics/demand-units?group=san_joaquin_valley
```

**Response:**
```json
{
  "demand_units": [
    {"du_id": "02_NU", "hydrologic_region": "Sacramento River", "community_agency": "Zone 02 Non-Project Urban"},
    {"du_id": "02_PU", "hydrologic_region": "Sacramento River", "community_agency": "Zone 02 Project Urban"},
    ...
  ]
}
```

#### Monthly Delivery Statistics
```
GET /api/statistics/scenarios/{scenario_id}/demand-units/delivery-monthly
GET /api/statistics/scenarios/{scenario_id}/demand-units/delivery-monthly?du_id=02_NU,02_PU
GET /api/statistics/scenarios/{scenario_id}/demand-units/delivery-monthly?group=sacramento_river
```

#### Monthly Shortage Statistics
```
GET /api/statistics/scenarios/{scenario_id}/demand-units/shortage-monthly
```

#### Period Summary
```
GET /api/statistics/scenarios/{scenario_id}/demand-units/period-summary
```

---

## Data Dictionary

### Water Month Convention
| Water Month | Calendar Month |
|-------------|----------------|
| 1 | October |
| 2 | November |
| 3 | December |
| 4 | January |
| 5 | February |
| 6 | March |
| 7 | April |
| 8 | May |
| 9 | June |
| 10 | July |
| 11 | August |
| 12 | September |

### Percentile Fields (for box plots)
| Field | Description |
|-------|-------------|
| q0 | 0th percentile (minimum) |
| q10 | 10th percentile |
| q30 | 30th percentile |
| q50 | 50th percentile (median) |
| q70 | 70th percentile |
| q90 | 90th percentile |
| q100 | 100th percentile (maximum) |

### Exceedance Fields (for exceedance plots)
| Field | Description |
|-------|-------------|
| exc_p5 | Value exceeded 5% of time (very wet/high delivery) |
| exc_p10 | Value exceeded 10% of time |
| exc_p25 | Value exceeded 25% of time |
| exc_p50 | Value exceeded 50% of time (median) |
| exc_p75 | Value exceeded 75% of time |
| exc_p90 | Value exceeded 90% of time |
| exc_p95 | Value exceeded 95% of time (very dry/low delivery) |

### Reliability Metrics
| Field | Description |
|-------|-------------|
| reliability_pct | Percentage of months meeting full demand (shortage = 0) |
| avg_pct_allocation_met | Average (delivery / demand) across all months × 100 |
| shortage_frequency_pct | Percentage of years/months with shortage > 0 |

---

## Visualization Recommendations

### 1. Percentile Band Chart (Monthly)
Use `q10`-`q90` for outer band, `q30`-`q70` for inner band, `q50` for median line.

### 2. Exceedance Curve
Plot exceedance percentiles (p5 through p95) to show probability distribution.

### 3. Dry Year Analysis
Filter period summary by `shortage_frequency_pct > threshold` or use lower exceedance percentiles (p75, p90, p95) to show dry-year deliveries.

### 4. Reliability Dashboard
- Show `reliability_pct` as primary metric
- Show `avg_pct_allocation_met` as secondary
- Show `shortage_frequency_pct` as risk indicator

---

## Implementation Status

| Component | Entity Data | Statistics Data | API Endpoints |
|-----------|-------------|-----------------|---------------|
| CWS Aggregates | Loaded (4) | Loaded (s0020) | Pending |
| M&I Contractors | Loaded (30) | Pending ETL | Pending |
| Demand Units | Loaded (125) | Pending ETL | Pending |

---

## Database Tables Reference

| Table | Purpose |
|-------|---------|
| `cws_aggregate_entity` | CWS aggregate definitions |
| `cws_aggregate_monthly` | Monthly delivery/shortage stats |
| `cws_aggregate_period_summary` | Period-of-record summary |
| `mi_contractor` | SWP contractor definitions |
| `mi_delivery_monthly` | Monthly delivery by contractor |
| `mi_shortage_monthly` | Monthly shortage by contractor |
| `mi_contractor_period_summary` | Period summary by contractor |
| `du_urban_entity` | Urban demand unit definitions |
| `du_delivery_monthly` | Monthly delivery by demand unit |
| `du_shortage_monthly` | Monthly shortage by demand unit |
| `du_period_summary` | Period summary by demand unit |
