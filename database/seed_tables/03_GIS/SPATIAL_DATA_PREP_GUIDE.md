# SPATIAL DATA PREPARATION GUIDE
## For Tier Map Visualization

### **📂 Directory: database/seed_tables/03_GIS/**

---

## **SUMMARY - WHAT WE HAVE:**

### **✅ TIER LOCATIONS FOUND IN NETWORK:**

**RES_STOR (Reservoir Storage):**
- ✅ SHSTA (Shasta) - Point in network_gis
- ✅ TRNTY (Trinity) - Point in network_gis
- ✅ OROVL (Oroville) - Point in network_gis
- ✅ FOLSM (Folsom) - Point in network_gis
- ✅ MELON (New Melones) - Point in network_gis
- ✅ MLRTN (Millerton) - Point in network_gis
- ✅ SLUIS (San Luis) - Point in network_gis (use for both CVP/SWP)

**ENV_FLOWS (Environmental Flows):**
- ✅ All 17 nodes found with GIS points:
  AMR004, FTR003, FTR029, MCD005, MOK028, SAC000, SAC049, SAC122, SAC148, 
  SAC257, SAC289, SJR070, SJR127, STS011, TRN111, TUO003, YUB002

**FW_EXP (Delta Exports):**
- ✅ CAA003 (Harvey O. Banks Pumping Plant) - Has GIS point
- ✅ DMC000 (C.W. "Bill" Jones Pumping Plant) - Has GIS point

---

## **❌ TIER LOCATIONS NOT IN NETWORK:**

**FW_DELTA_USES (In-Delta Uses):**
- ❌ Jersey Point (JP) - Need to add manually
- ❌ Emmaton (EM) - Need to add manually

**Coordinates (approximate):**
- Jersey Point: 38.056°N, 121.745°W
- Emmaton: 38.053°N, 121.694°W

---

## **🗺️ ADDITIONAL SPATIAL DATA AVAILABLE:**

### **1. Reservoir Polygons** 
**Source:** `data/raw/from_geopackage/GIS_coords_from_other_sources/reservoirs_from_nhd.csv`

**Contains:**
- Shasta Lake: 3 polygons (use 50.25 km² - largest)
- Folsom Lake: 2 polygons (use 27.68 km² - largest)
- Trinity Lake, Oroville, New Melones, Millerton, San Luis: 1 polygon each

**Units:** 
- Area: km² (square kilometers)
- Elevation: meters
- WKT: Polygon geometries in SRID 4326

### **2. WBA (Aquifer) Polygons**
**Source:** `data/raw/from_geopackage/wba_4326.csv`

**Contains:**
- 42 Water Budget Area polygons
- WBA_IDs: DETAW, 02, 03, 04, 05, 06, 07N, 07S, 08N, 08S, etc.

**Units:**
- GIS_Acres: acres
- Shape_Area: square degrees (needs conversion)
- WKT: Polygon geometries in SRID 4326

---

## **📋 FILES TO CREATE:**

### **1. reservoirs.csv** → `03_GIS/reservoirs.csv`

```csv
calsim_short_code,reservoir_name,geom_wkt,srid,area_sqkm,elevation_m,data_source
SHSTA,Shasta Lake,MULTIPOLYGON(...),4326,50.25,324.6,NHD
TRNTY,Trinity Lake,MULTIPOLYGON(...),4326,63.08,XXX,NHD
OROVL,Lake Oroville,MULTIPOLYGON(...),4326,42.94,XXX,NHD
FOLSM,Folsom Lake,MULTIPOLYGON(...),4326,27.68,XXX,NHD
MELON,New Melones Lake,MULTIPOLYGON(...),4326,36.25,XXX,NHD
MLRTN,Millerton Lake,MULTIPOLYGON(...),4326,13.41,XXX,NHD
SLUIS,San Luis Reservoir,MULTIPOLYGON(...),4326,51.93,XXX,NHD
```

**Extract:** Largest polygon for Shasta and Folsom, single polygon for others

### **2. wba.csv** → `03_GIS/wba.csv`

```csv
wba_id,wba_name,geom_wkt,srid,area_acres,hydrologic_region,data_source
DETAW,DETAW,MULTIPOLYGON(...),4326,XXX,DELTA,CalSim_Geopackage
02,WBA 02,MULTIPOLYGON(...),4326,XXX,SAC,CalSim_Geopackage
03,WBA 03,MULTIPOLYGON(...),4326,XXX,SAC,CalSim_Geopackage
...
```

**Extract:** All 42 WBAs from wba_4326.csv

### **3. compliance_stations.csv** → `03_GIS/compliance_stations.csv`

```csv
station_code,station_name,latitude,longitude,srid,tier_use,data_source
JP,Jersey Point,38.056,-121.745,4326,FW_DELTA_USES,Manual
EM,Emmaton,38.053,-121.694,4326,FW_DELTA_USES,Manual
```

**Create:** Manually with researched coordinates

---

## **🚀 NEXT STEPS:**

**I can create Python scripts to:**
1. Extract largest reservoir polygons → reservoirs.csv
2. Process WBA data → wba.csv  
3. Create compliance stations file
4. Create SQL loading scripts for all 3 tables

**Ready to proceed?**



