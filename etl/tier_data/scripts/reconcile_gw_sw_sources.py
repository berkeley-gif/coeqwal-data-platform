#!/usr/bin/env python3
"""reconcile_gw_sw_sources.py - Compare gw/sw classification across reference sources.

Urban sources:
  - Seed: database/seed_tables/04_calsim_data/du_urban_entity.csv
  - M&I team xlsx: etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx
    (columns gw_su, sw_du, optional Notes)
  - CalSim PDF flat extract (partial): data/raw/csv_from_CalSim_report_pdf/du+diversion/
    urban_du_calsim_report.csv (community rows; OR rollup computed in-script)
  - CalSim PDF rollup file (partial): urban_du_calsim_report_rollup.csv

Ag sources (only PDF tables that include gw/sw columns):
  - Seed: database/seed_tables/04_calsim_data/du_agriculture_entity.csv
  - SAC Table 3-3: ag_demand_units_sac_calsim_report_Table_3-3.csv
  - SJR Table 3-6: ag_demand_units_sjr_calsim_report_Table_3-6.csv

Ag PDF tables 3-4 and 3-5 list diversion arcs only. They have no gw/sw
columns and are not compared here.

Urban disagreements are usually semantic (rollup rule, team overrides),
not formatting. Both seed and xlsx use clean '0'/'1' strings.

Usage:
    python etl/tier_data/scripts/reconcile_gw_sw_sources.py
    python etl/tier_data/scripts/reconcile_gw_sw_sources.py --csv-out /tmp/gw_sw_audit.csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
SEED_URBAN = REPO / "database/seed_tables/04_calsim_data/du_urban_entity.csv"
XLSX_URBAN = REPO / "etl/tier_data/reference/Final_M&Idemandunits_withlatlongs.xlsx"
SEED_AG = REPO / "database/seed_tables/04_calsim_data/du_agriculture_entity.csv"
PDF_DIR = REPO / "data/raw/csv_from_CalSim_report_pdf/du+diversion"
PDF_URBAN_FLAT = PDF_DIR / "urban_du_calsim_report.csv"
PDF_URBAN_ROLLUP = PDF_DIR / "urban_du_calsim_report_rollup.csv"
PDF_AG_SAC = PDF_DIR / "ag_demand_units_sac_calsim_report_Table_3-3.csv"
PDF_AG_SJR = PDF_DIR / "ag_demand_units_sjr_calsim_report_Table_3-6.csv"

AG_PDF_WITH_GW_SW = (
    ("SAC Table 3-3", PDF_AG_SAC, "demand_unit"),
    ("SJR Table 3-6", PDF_AG_SJR, "demand_unit"),
)

AG_PDF_NO_GW_SW = (
    "nonproject_ag_diversions_sac_river_Table_3-4.csv",
    "nondistrict_ag_diversions_feather_river_Table_3-5.csv",
)


def _norm_flag(value: object) -> str:
    if value is None or value == "":
        return ""
    if isinstance(value, bool):
        return "1" if value else "0"
    s = str(value).strip()
    if s in ("0", "1"):
        return s
    if s.lower() in ("true", "yes"):
        return "1"
    if s.lower() in ("false", "no"):
        return "0"
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def _load_seed_urban() -> dict[str, tuple[str, str, str]]:
    out: dict[str, tuple[str, str, str]] = {}
    with open(SEED_URBAN, newline="") as f:
        for row in csv.DictReader(f):
            out[row["DU_ID"]] = (
                _norm_flag(row.get("gw")),
                _norm_flag(row.get("sw")),
                row.get("source", ""),
            )
    return out


def _load_xlsx_urban() -> tuple[dict[str, tuple[str, str]], dict[str, str]]:
    try:
        import openpyxl
    except ImportError as e:
        raise SystemExit(
            "openpyxl required. Install in venv or use scripts/pdf_scraper/venv."
        ) from e
    wb = openpyxl.load_workbook(XLSX_URBAN, data_only=True)
    ws = wb.active
    flags: dict[str, tuple[str, str]] = {}
    notes: dict[str, str] = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[0]:
            continue
        du_id = str(row[0]).strip()
        flags[du_id] = (_norm_flag(row[1]), _norm_flag(row[2]))
        note = row[7]
        if note and isinstance(note, str) and len(note.strip()) > 15:
            notes[du_id] = note.strip()
    return flags, notes


def _or_rollup_from_flat(path: Path) -> dict[str, tuple[str, str, int]]:
    """OR across community rows: gw=1 if any community gw=1, same for sw."""
    if not path.exists():
        return {}
    by_du: dict[str, list[tuple[str, str]]] = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            du_id = row["du_id"].strip()
            by_du[du_id].append(
                (_norm_flag(row.get("gw_bool")), _norm_flag(row.get("sw_bool")))
            )
    out: dict[str, tuple[str, str, int]] = {}
    for du_id, flags in by_du.items():
        gw = "1" if any(g == "1" for g, _ in flags) else "0"
        sw = "1" if any(s == "1" for _, s in flags) else "0"
        out[du_id] = (gw, sw, len(flags))
    return out


def _load_pdf_rollup(path: Path) -> dict[str, tuple[str, str, int]]:
    if not path.exists():
        return {}
    out: dict[str, tuple[str, str, int]] = {}
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            du_id = row["du_id"].strip()
            n = int(row.get("n_systems_pdf") or 0)
            out[du_id] = (
                _norm_flag(row.get("gw_pdf")),
                _norm_flag(row.get("sw_pdf")),
                n,
            )
    return out


def _classify_urban_disagreement(
    seed_gw: str,
    seed_sw: str,
    xlsx_gw: str,
    xlsx_sw: str,
) -> str:
    if seed_gw == "" or seed_sw == "":
        return "seed_empty"
    if xlsx_gw == "1" and seed_gw == "0" and xlsx_sw == seed_sw:
        return "xlsx_adds_gw"
    if xlsx_sw == "0" and seed_sw == "1" and xlsx_gw == seed_gw:
        return "xlsx_clears_sw"
    return "mixed"


def _compare_urban(
    seed: dict[str, tuple[str, str, str]],
    xlsx: dict[str, tuple[str, str]],
    notes: dict[str, str],
    pdf_or: dict[str, tuple[str, str, int]],
) -> list[dict[str, object]]:
    both = sorted(set(seed) & set(xlsx))
    seed_only = sorted(set(seed) - set(xlsx))
    xlsx_only = sorted(set(xlsx) - set(seed))
    agree_count = disagree_count = 0
    rows: list[dict[str, object]] = []

    for du_id in both:
        seed_gw, seed_sw, src = seed[du_id]
        xlsx_gw, xlsx_sw = xlsx[du_id]
        pdf_gw, pdf_sw, pdf_n = pdf_or.get(du_id, ("", "", 0))
        pattern = ""
        if (seed_gw, seed_sw) == (xlsx_gw, xlsx_sw):
            agree_count += 1
        else:
            disagree_count += 1
            pattern = _classify_urban_disagreement(seed_gw, seed_sw, xlsx_gw, xlsx_sw)

        rows.append(
            {
                "du_id": du_id,
                "seed_gw": seed_gw,
                "seed_sw": seed_sw,
                "xlsx_gw_su": xlsx_gw,
                "xlsx_sw_du": xlsx_sw,
                "pdf_or_gw": pdf_gw,
                "pdf_or_sw": pdf_sw,
                "pdf_n_systems": pdf_n,
                "in_pdf_extract": du_id in pdf_or,
                "seed_source": src,
                "xlsx_note": notes.get(du_id, ""),
                "pattern": pattern,
                "seed_xlsx_agree": (seed_gw, seed_sw) == (xlsx_gw, xlsx_sw),
                "xlsx_pdf_agree": (xlsx_gw, xlsx_sw) == (pdf_gw, pdf_sw)
                if pdf_gw != ""
                else None,
                "seed_pdf_agree": (seed_gw, seed_sw) == (pdf_gw, pdf_sw)
                if pdf_gw != ""
                else None,
            }
        )

    print("=== Urban gw/sw: seed CSV vs M&I xlsx ===")
    print(f"  seed rows:        {len(seed)}")
    print(f"  xlsx rows:        {len(xlsx)}")
    print(f"  in both:          {len(both)}")
    print(f"  agree:            {agree_count}")
    print(f"  disagree:         {disagree_count}")
    print(f"  seed-only:        {len(seed_only)}")
    print(f"  xlsx-only:        {len(xlsx_only)}")
    if seed_only:
        print(f"    {seed_only[:12]}{'...' if len(seed_only) > 12 else ''}")
    if xlsx_only:
        print(f"    {xlsx_only}")

    pdf_ids = set(pdf_or)
    disagree_ids = [r["du_id"] for r in rows if not r["seed_xlsx_agree"]]
    in_pdf = [d for d in disagree_ids if d in pdf_ids]
    print(f"\n  PDF flat/OR extract covers {len(pdf_ids)} du_ids "
          f"(of {len(seed)} seed, {disagree_count} disagreements)")
    print(f"  Disagreements with PDF row: {len(in_pdf)} of {len(disagree_ids)}")

    patterns: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["pattern"]:
            patterns[str(r["pattern"])] += 1
    if patterns:
        print("\n  Disagreement patterns:")
        for k, v in sorted(patterns.items()):
            print(f"    {k}: {v}")

    resolvable = [
        r
        for r in rows
        if not r["seed_xlsx_agree"]
        and r["in_pdf_extract"]
        and r["xlsx_pdf_agree"] is True
        and r["seed_pdf_agree"] is False
    ]
    if resolvable:
        print("\n  Likely seed fixes (xlsx and PDF OR agree, seed differs):")
        for r in resolvable:
            print(
                f"    {r['du_id']:14s} seed=({r['seed_gw']},{r['seed_sw']}) "
                f"-> ({r['xlsx_gw_su']},{r['xlsx_sw_du']}) "
                f"[pdf n={r['pdf_n_systems']}]"
            )

    noted = [r for r in rows if r["xlsx_note"] and not r["seed_xlsx_agree"]]
    if noted:
        print("\n  Disagreements with xlsx Notes (team override context):")
        for r in noted:
            print(f"    {r['du_id']:14s} seed=({r['seed_gw']},{r['seed_sw']}) "
                  f"xlsx=({r['xlsx_gw_su']},{r['xlsx_sw_du']})")
            print(f"      {str(r['xlsx_note'])[:120]}")

    if disagree_count:
        print("\n  First 20 disagreements "
              "(du_id, seed, xlsx, pdf_or, pattern):")
        shown = 0
        for r in rows:
            if r["seed_xlsx_agree"]:
                continue
            pdf = (
                f"({r['pdf_or_gw']},{r['pdf_or_sw']})"
                if r["in_pdf_extract"]
                else "n/a"
            )
            print(
                f"    {r['du_id']:14s} seed=({r['seed_gw']},{r['seed_sw']}) "
                f"xlsx=({r['xlsx_gw_su']},{r['xlsx_sw_du']}) pdf_or={pdf} "
                f"{r['pattern']}"
            )
            shown += 1
            if shown >= 20:
                rest = disagree_count - shown
                if rest > 0:
                    print(f"    ... +{rest} more (use --csv-out for full list)")
                break

    return rows


def _compare_ag_pdf(label: str, path: Path, id_col: str) -> None:
    if not path.exists():
        print(f"\n=== Ag {label}: skipped (missing {path.name}) ===")
        return
    if not SEED_AG.exists():
        print(f"\n=== Ag {label}: skipped (missing seed) ===")
        return
    with open(path, newline="") as f:
        pdf_rows = {
            r[id_col]: (_norm_flag(r["gw"]), _norm_flag(r["sw"]))
            for r in csv.DictReader(f)
        }
    with open(SEED_AG, newline="") as f:
        seed_rows = {
            r["DU_ID"]: (_norm_flag(r.get("gw")), _norm_flag(r.get("sw")))
            for r in csv.DictReader(f)
        }
    overlap = set(pdf_rows) & set(seed_rows)
    agree = sum(1 for d in overlap if pdf_rows[d] == seed_rows[d])
    disagree = len(overlap) - agree
    print(f"\n=== Ag gw/sw: {label} vs seed (overlap only) ===")
    print(f"  PDF rows:      {len(pdf_rows)}")
    print(f"  seed ag rows:  {len(seed_rows)}")
    print(f"  overlap:       {len(overlap)}")
    print(f"  agree:         {agree}")
    print(f"  disagree:      {disagree}")
    if disagree:
        for du_id in sorted(overlap):
            if pdf_rows[du_id] != seed_rows[du_id]:
                print(
                    f"    {du_id}: seed={seed_rows[du_id]} "
                    f"pdf={pdf_rows[du_id]}"
                )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv-out",
        type=Path,
        help="Write full urban reconciliation matrix to CSV",
    )
    args = parser.parse_args()

    if not SEED_URBAN.exists():
        print(f"ERROR: missing {SEED_URBAN}", file=sys.stderr)
        return 1
    if not XLSX_URBAN.exists():
        print(f"ERROR: missing {XLSX_URBAN}", file=sys.stderr)
        return 1

    seed = _load_seed_urban()
    xlsx, notes = _load_xlsx_urban()

    pdf_or = _or_rollup_from_flat(PDF_URBAN_FLAT)
    for du_id, (gw, sw, n) in _load_pdf_rollup(PDF_URBAN_ROLLUP).items():
        pdf_or.setdefault(du_id, (gw, sw, n))

    urban_rows = _compare_urban(seed, xlsx, notes, pdf_or)

    for label, path, id_col in AG_PDF_WITH_GW_SW:
        _compare_ag_pdf(label, path, id_col)

    print("\n=== Ag PDF tables without gw/sw (not compared) ===")
    for name in AG_PDF_NO_GW_SW:
        present = (PDF_DIR / name).exists()
        print(f"  {name}: {'present' if present else 'missing'} "
              "(diversion arcs only)")

    if args.csv_out:
        fieldnames = [
            "du_id",
            "seed_gw",
            "seed_sw",
            "xlsx_gw_su",
            "xlsx_sw_du",
            "pdf_or_gw",
            "pdf_or_sw",
            "pdf_n_systems",
            "in_pdf_extract",
            "seed_xlsx_agree",
            "xlsx_pdf_agree",
            "seed_pdf_agree",
            "pattern",
            "seed_source",
            "xlsx_note",
        ]
        with open(args.csv_out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            w.writerows(urban_rows)
        print(f"\nWrote {len(urban_rows)} urban rows to {args.csv_out}")

    print(
        "\nNext: complete urban Table 3-7 PDF extract (see docs/gw_sw_reconciliation.md), "
        "resolve disagreements case by case, then update seed and run BOOL migration."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
