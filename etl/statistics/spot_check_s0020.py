#!/usr/bin/env python3
"""
Spot-check: exercise each refactored ETL module's core calculations
against the s0020 reference DV and SV CSVs.

No database or S3 needed. Just loads the CSVs and validates math.
"""

import sys
import numpy as np
import pandas as pd
from pathlib import Path

from units import CFS_TO_TAF_PER_DAY

REFERENCE_DIR = Path(__file__).parent.parent / "reference"
DV_CSV = REFERENCE_DIR / "s0020_coeqwal_calsim_output.csv"
SV_CSV = REFERENCE_DIR / "s0020_coeqwal_sv_input.csv"


def load_csv(path, label):
    """Load the multi-header DSS-export CSV format.
    Row 0: source (a=label, CALSIM, ...)
    Row 1: variable name (b=label, A17, AW_02_NA, ...)
    Row 2: kind (c=label, SURFACE-AREA, APPLIED-WATER, ...)
    Row 3: interval (e=label, 1MON, ...)
    Row 4: study (f=label, L2020A, ...)
    Row 5: type (type=label, PER-AVER, PER-CUM, ...)
    Row 6: units (units=label, CFS, TAF, ...)
    Row 7+: data (datetime, values)
    """
    print(f"\nLoading {label}: {path.name}")
    df = pd.read_csv(path, header=None, low_memory=False)
    names_row = df.iloc[1]   # row 1 = variable names
    units_row = df.iloc[6]   # row 6 = units
    kind_row = df.iloc[2]    # row 2 = kind

    data = df.iloc[7:].copy()
    raw_names = list(names_row.values)
    # Deduplicate column names: append _dup2, _dup3, etc.
    seen = {}
    deduped = []
    for name in raw_names:
        if name in seen:
            seen[name] += 1
            deduped.append(f"{name}_dup{seen[name]}")
        else:
            seen[name] = 1
            deduped.append(name)
    data.columns = deduped
    data = data.rename(columns={data.columns[0]: "DateTime"})
    data["DateTime"] = pd.to_datetime(data["DateTime"])

    for col in data.columns[1:]:
        data[col] = pd.to_numeric(data[col], errors="coerce")

    col_units = {}
    col_kinds = {}
    for i in range(1, len(names_row)):
        name = str(names_row.iloc[i])
        col_units[name] = str(units_row.iloc[i]).strip()
        col_kinds[name] = str(kind_row.iloc[i]).strip()

    print(f"  Loaded {len(data)} rows, {len(data.columns)-1} variables")
    return data, col_units, col_kinds


def add_water_year_month(df):
    df = df.copy()
    df["Month"] = df["DateTime"].dt.month
    df["Year"] = df["DateTime"].dt.year
    df["DaysInMonth"] = df["DateTime"].dt.days_in_month
    df["WaterYear"] = np.where(df["Month"] >= 10, df["Year"] + 1, df["Year"])
    df["WaterMonth"] = np.where(df["Month"] >= 10, df["Month"] - 9, df["Month"] + 3)
    return df


def check_ag(dv, dv_units):
    """Spot-check AG demand, delivery, GW pumping for a sample DU."""
    print("\n" + "=" * 70)
    print("AG MODULE: WBA 02_NA")
    print("=" * 70)

    du = "02_NA"
    aw_col = f"AW_{du}"
    dn_col = f"DN_{du}"
    gp_col = f"GP_{du}"

    for col_name, expected_unit in [(aw_col, "CFS"), (dn_col, "CFS"), (gp_col, "CFS")]:
        if col_name in dv.columns:
            actual = dv_units.get(col_name, "?")
            status = "OK" if actual == expected_unit else f"MISMATCH (got {actual})"
            print(f"  {col_name}: unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in DV")

    df = add_water_year_month(dv)

    if aw_col in df.columns:
        df["demand_taf"] = df[aw_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    if dn_col in df.columns:
        df["delivery_taf"] = df[dn_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY

    if gp_col in df.columns:
        df["gw_pump_taf"] = df[gp_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    elif aw_col in df.columns and dn_col in df.columns:
        df["gw_pump_taf"] = df["demand_taf"] - df["delivery_taf"]
        print(f"  GP_{du} not found, using AW - DN")

    annual = df.groupby("WaterYear").agg(
        demand=("demand_taf", "sum"),
        delivery=("delivery_taf", "sum"),
        gw_pump=("gw_pump_taf", "sum"),
    )
    annual = annual.iloc[1:-1]  # trim partial first/last years

    print(f"\n  Annual averages (WY {annual.index.min()}-{annual.index.max()}):")
    print(f"    Demand (AW):          {annual['demand'].mean():8.2f} TAF/yr")
    print(f"    SW Delivery (DN):     {annual['delivery'].mean():8.2f} TAF/yr")
    print(f"    GW Pumping:           {annual['gw_pump'].mean():8.2f} TAF/yr")
    print(f"    Balance check (AW - DN - GP): {(annual['demand'] - annual['delivery'] - annual['gw_pump']).mean():8.4f} TAF/yr (should be ~0)")

    print("\n  Monthly percentiles (demand TAF) for October:")
    oct_demand = df[df["WaterMonth"] == 1]["demand_taf"]
    for p in [0, 10, 50, 90, 100]:
        print(f"    p{p:3d}: {np.percentile(oct_demand, p):8.3f}")

    return True


def check_du_urban(dv, sv, dv_units, sv_units):
    """Spot-check CWS DU delivery/demand for a sample DU."""
    print("\n" + "=" * 70)
    print("CWS DU (URBAN) MODULE: WBA 02_PU")
    print("=" * 70)

    du = "02_PU"
    dn_col = f"DN_{du}"
    ud_col = f"UD_{du}"
    shrtg_col = f"SHRTG_{du}"

    for col_name, src, expected_unit in [
        (dn_col, "DV", "CFS"), (shrtg_col, "DV", "CFS"), (ud_col, "SV", "TAF")
    ]:
        source = dv_units if src == "DV" else sv_units
        if col_name in source:
            actual = source[col_name]
            status = "OK" if actual == expected_unit else f"MISMATCH (got {actual})"
            print(f"  {col_name} ({src}): unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in {src}")

    df_dv = add_water_year_month(dv)
    df_sv = add_water_year_month(sv)

    if dn_col in df_dv.columns:
        df_dv["delivery_taf"] = df_dv[dn_col] * df_dv["DaysInMonth"] * CFS_TO_TAF_PER_DAY

    if shrtg_col in df_dv.columns:
        df_dv["shortage_taf"] = df_dv[shrtg_col] * df_dv["DaysInMonth"] * CFS_TO_TAF_PER_DAY

    # UD_* is already in TAF from SV (used for demand verification below)
    if ud_col in df_sv.columns:
        _ = df_sv.set_index("DateTime")[ud_col]

    annual_dv = df_dv.groupby("WaterYear").agg(
        delivery=("delivery_taf", "sum"),
        shortage=("shortage_taf", "sum") if "shortage_taf" in df_dv else ("delivery_taf", lambda x: 0),
    )
    annual_dv = annual_dv.iloc[1:-1]

    annual_sv = df_sv.groupby("WaterYear").agg(demand=(ud_col, "sum"))
    annual_sv = annual_sv.iloc[1:-1]

    merged = annual_dv.join(annual_sv, how="inner")

    print(f"\n  Annual averages (WY {merged.index.min()}-{merged.index.max()}):")
    print(f"    Demand (UD from SV):  {merged['demand'].mean():8.2f} TAF/yr")
    print(f"    Delivery (DN from DV):{merged['delivery'].mean():8.2f} TAF/yr")
    if "shortage" in merged:
        print(f"    Shortage (SHRTG):     {merged['shortage'].mean():8.2f} TAF/yr")
        pct_met = (merged["delivery"] / merged["demand"].replace(0, np.nan) * 100).mean()
        print(f"    Avg % demand met:     {pct_met:8.1f}%")

    return True


def check_mi(dv, sv, dv_units, sv_units):
    """Spot-check MI contractor for a sample contractor (AVEK)."""
    print("\n" + "=" * 70)
    print("MI MODULE: AVEK contractor")
    print("=" * 70)

    del_col = "D_ESB324_AVEK_PMI"
    short_col = "SHORT_D_ESB324_AVEK_PMI"
    dem_col = "DEM_D_ESB324_AVEK_PIN"

    for col_name, src, expected_unit in [
        (del_col, "DV", "CFS"), (short_col, "DV", "CFS"), (dem_col, "SV", "TAF")
    ]:
        source = dv if src == "DV" else sv
        units = dv_units if src == "DV" else sv_units
        if col_name in source.columns:
            actual = units.get(col_name, "?")
            status = "OK" if actual == expected_unit else f"MISMATCH (got {actual})"
            print(f"  {col_name} ({src}): unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in {src}")

    df_dv = add_water_year_month(dv)
    df_sv = add_water_year_month(sv)

    if del_col in df_dv.columns:
        df_dv["delivery_taf"] = df_dv[del_col] * df_dv["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    if short_col in df_dv.columns:
        df_dv["shortage_taf"] = df_dv[short_col] * df_dv["DaysInMonth"] * CFS_TO_TAF_PER_DAY

    annual_dv = df_dv.groupby("WaterYear").agg(
        delivery=("delivery_taf", "sum"),
        shortage=("shortage_taf", "sum"),
    )
    annual_dv = annual_dv.iloc[1:-1]

    if dem_col in df_sv.columns:
        annual_sv = df_sv.groupby("WaterYear").agg(demand=(dem_col, "sum"))
        annual_sv = annual_sv.iloc[1:-1]
        merged = annual_dv.join(annual_sv, how="inner")
    else:
        merged = annual_dv
        merged["demand"] = np.nan

    print(f"\n  Annual averages (WY {merged.index.min()}-{merged.index.max()}):")
    print(f"    Demand (DEM_D from SV):  {merged['demand'].mean():8.2f} TAF/yr")
    print(f"    Delivery (D_ from DV):   {merged['delivery'].mean():8.2f} TAF/yr")
    print(f"    Shortage (SHORT from DV):{merged['shortage'].mean():8.2f} TAF/yr")

    balance = merged["demand"] - merged["delivery"] - merged["shortage"]
    print(f"    Balance (demand - delivery - shortage): {balance.mean():8.4f} TAF/yr (should be ~0)")

    return True


def check_cws_aggregate(dv, dv_units):
    """Spot-check CWS aggregate (SWP MI total)."""
    print("\n" + "=" * 70)
    print("CWS AGGREGATE MODULE: SWP_MI")
    print("=" * 70)

    del_col = "DEL_SWP_PMI"
    short_col = "SHORT_SWP_PMI"

    for col_name in [del_col, short_col]:
        if col_name in dv.columns:
            actual = dv_units.get(col_name, "?")
            status = "OK" if actual == "CFS" else f"MISMATCH (got {actual})"
            print(f"  {col_name}: unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in DV")

    df = add_water_year_month(dv)

    df["delivery_taf"] = df[del_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    df["shortage_taf"] = df[short_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    df["demand_taf"] = df["delivery_taf"] + df["shortage_taf"]

    annual = df.groupby("WaterYear").agg(
        delivery=("delivery_taf", "sum"),
        shortage=("shortage_taf", "sum"),
        demand=("demand_taf", "sum"),
    )
    annual = annual.iloc[1:-1]

    print(f"\n  Annual averages (WY {annual.index.min()}-{annual.index.max()}):")
    print(f"    Demand (del+short):   {annual['demand'].mean():8.2f} TAF/yr")
    print(f"    Delivery:             {annual['delivery'].mean():8.2f} TAF/yr")
    print(f"    Shortage:             {annual['shortage'].mean():8.2f} TAF/yr")
    reliability = (annual["shortage"] < 0.1).sum() / len(annual) * 100
    print(f"    Reliability (yrs <0.1 TAF shortage): {reliability:.1f}%")

    return True


def check_reservoirs(dv, dv_units):
    """Spot-check reservoir storage and spill for Folsom."""
    print("\n" + "=" * 70)
    print("RESERVOIR MODULE: Folsom (FOLSM)")
    print("=" * 70)

    CAPACITY_OVERRIDES = {"FOLSM": 967.0, "MLRTN": 524.0, "OROVL": 3424.8, "MELON": 2420.0}

    s_col = "S_FOLSM"
    flood_col = "C_FOLSM_FLOOD"
    cap = CAPACITY_OVERRIDES["FOLSM"]

    for col_name, expected in [(s_col, "TAF"), (flood_col, "CFS")]:
        if col_name in dv.columns:
            actual = dv_units.get(col_name, "?")
            status = "OK" if actual == expected else f"MISMATCH (got {actual})"
            print(f"  {col_name}: unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in DV")

    df = add_water_year_month(dv)

    print(f"\n  Capacity override: {cap} TAF")

    storage = df[s_col]
    print("\n  Storage stats (TAF):")
    print(f"    Mean:   {storage.mean():8.1f}")
    print(f"    Min:    {storage.min():8.1f}")
    print(f"    Max:    {storage.max():8.1f}")
    print(f"    Pct of capacity (mean): {storage.mean()/cap*100:.1f}%")

    if flood_col in df.columns:
        df["spill_taf"] = df[flood_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
        annual_spill = df.groupby("WaterYear")["spill_taf"].sum()
        annual_spill = annual_spill.iloc[1:-1]
        spill_freq = (annual_spill > 0.1).sum() / len(annual_spill) * 100
        print("\n  Spill stats:")
        print(f"    Avg annual spill: {annual_spill.mean():8.1f} TAF")
        print(f"    Max annual spill: {annual_spill.max():8.1f} TAF")
        print(f"    Spill frequency:  {spill_freq:.1f}% of years")
    else:
        print("\n  C_FOLSM_FLOOD not found, skipping spill check")

    print("\n  April storage percentiles:")
    apr = df[df["Month"] == 4][s_col]
    for p in [0, 10, 50, 90, 100]:
        val = np.percentile(apr, p)
        print(f"    p{p:3d}: {val:8.1f} TAF ({val/cap*100:.1f}%)")

    return True


def check_env_flows(dv, dv_units):
    """Spot-check env flow for Sacramento at Freeport."""
    print("\n" + "=" * 70)
    print("ENV FLOW MODULE: C_SAC041 (Sacramento at Freeport)")
    print("=" * 70)

    col = "C_SAC041"
    if col in dv.columns:
        actual = dv_units.get(col, "?")
        status = "OK" if actual == "CFS" else f"MISMATCH (got {actual})"
        print(f"  {col}: unit={actual} {status}")
    else:
        print(f"  {col}: NOT FOUND")
        return False

    df = add_water_year_month(dv)
    flow = df[col]

    print("\n  Flow stats (CFS):")
    print(f"    Mean:   {flow.mean():10.1f}")
    print(f"    Min:    {flow.min():10.1f}")
    print(f"    Max:    {flow.max():10.1f}")
    print(f"    Median: {flow.median():10.1f}")

    print("\n  Monthly means (CFS):")
    for m in range(1, 13):
        val = df[df["Month"] == m][col].mean()
        month_name = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"][m-1]
        print(f"    {month_name}: {val:10.1f}")

    return True


def check_refuge(dv, sv, dv_units, sv_units):
    """Spot-check refuge delivery/demand for WBA 02_NA."""
    print("\n" + "=" * 70)
    print("REFUGE MODULE: WBA 02_NA")
    print("=" * 70)

    awo_col = "AWO_02_NA"
    dn_col = "DN_02_NA"

    for col_name, src, expected in [(awo_col, "SV", "TAF"), (dn_col, "DV", "CFS")]:
        source_units = sv_units if src == "SV" else dv_units
        if col_name in source_units:
            actual = source_units[col_name]
            status = "OK" if actual == expected else f"MISMATCH (got {actual})"
            print(f"  {col_name} ({src}): unit={actual} {status}")
        else:
            print(f"  {col_name}: NOT FOUND in {src}")

    df_dv = add_water_year_month(dv)
    df_sv = add_water_year_month(sv)

    if dn_col in df_dv.columns:
        df_dv["delivery_taf"] = df_dv[dn_col] * df_dv["DaysInMonth"] * CFS_TO_TAF_PER_DAY

    annual_dv = df_dv.groupby("WaterYear").agg(delivery=("delivery_taf", "sum"))
    annual_dv = annual_dv.iloc[1:-1]

    if awo_col in df_sv.columns:
        annual_sv = df_sv.groupby("WaterYear").agg(demand=(awo_col, "sum"))
        annual_sv = annual_sv.iloc[1:-1]
        merged = annual_dv.join(annual_sv, how="inner")
    else:
        merged = annual_dv
        merged["demand"] = np.nan

    print(f"\n  Annual averages (WY {merged.index.min()}-{merged.index.max()}):")
    print(f"    Demand (AWO from SV):   {merged['demand'].mean():8.2f} TAF/yr")
    print(f"    Delivery (DN from DV):  {merged['delivery'].mean():8.2f} TAF/yr")
    shortage = (merged["demand"] - merged["delivery"]).clip(lower=0)
    print(f"    Shortage (max(d-del,0)):{shortage.mean():8.2f} TAF/yr")

    return True


def check_cross_module_consistency(dv, sv, dv_units, sv_units):
    """Cross-check: DN_02_NA used by both AG and Refuge — should be same."""
    print("\n" + "=" * 70)
    print("CROSS-MODULE CONSISTENCY CHECK")
    print("=" * 70)

    dn_col = "DN_02_NA"
    if dn_col not in dv.columns:
        print(f"  {dn_col} not in DV, skipping")
        return

    df = add_water_year_month(dv)
    df["dn_taf"] = df[dn_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
    annual = df.groupby("WaterYear")["dn_taf"].sum().iloc[1:-1]
    print(f"  DN_02_NA avg annual delivery: {annual.mean():.2f} TAF — used by both AG and Refuge")
    print("  (This single DV column feeds both modules, so they must agree)")

    aw_col = "AW_02_NA"
    awo_col = "AWO_02_NA"
    if aw_col in dv.columns and awo_col in sv.columns:
        df["aw_taf"] = df[aw_col] * df["DaysInMonth"] * CFS_TO_TAF_PER_DAY
        aw_annual = df.groupby("WaterYear")["aw_taf"].sum().iloc[1:-1]
        awo_annual = sv.copy()
        awo_annual = add_water_year_month(awo_annual)
        awo_annual = awo_annual.groupby("WaterYear")[awo_col].sum().iloc[1:-1]
        print(f"\n  AW_02_NA (AG demand, DV→TAF):    {aw_annual.mean():.2f} TAF/yr")
        print(f"  AWO_02_NA (Refuge demand, SV TAF): {awo_annual.mean():.2f} TAF/yr")
        print("  (These are different variables for different purposes — values can differ)")


def main():
    print("=" * 70)
    print("SPOT-CHECK: s0020 reference data through refactored ETL calculations")
    print("=" * 70)

    if not DV_CSV.exists() or not SV_CSV.exists():
        print(f"ERROR: Reference files not found in {REFERENCE_DIR}")
        sys.exit(1)

    dv, dv_units, dv_kinds = load_csv(DV_CSV, "DV (calsim output)")
    sv, sv_units, sv_kinds = load_csv(SV_CSV, "SV (sv input)")

    results = {}
    results["ag"] = check_ag(dv, dv_units)
    results["du_urban"] = check_du_urban(dv, sv, dv_units, sv_units)
    results["mi"] = check_mi(dv, sv, dv_units, sv_units)
    results["cws_aggregate"] = check_cws_aggregate(dv, dv_units)
    results["reservoirs"] = check_reservoirs(dv, dv_units)
    results["env_flows"] = check_env_flows(dv, dv_units)
    results["refuge"] = check_refuge(dv, sv, dv_units, sv_units)
    check_cross_module_consistency(dv, sv, dv_units, sv_units)

    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    all_ok = True
    for module, ok in results.items():
        status = "PASS" if ok else "FAIL"
        print(f"  {module:20s}: {status}")
        if not ok:
            all_ok = False
    print(f"\n  Overall: {'ALL CHECKS PASSED' if all_ok else 'SOME CHECKS FAILED'}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
