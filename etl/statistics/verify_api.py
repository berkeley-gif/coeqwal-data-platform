#!/usr/bin/env python3
"""
verify_api.py - API verification for the COEQWAL data explorer.

Queries API endpoints and compares responses against direct database queries
to confirm the API layer is not introducing data errors.

Usage:
    python verify_api.py --scenario s0020
    python verify_api.py --scenario s0020 --api-url http://localhost:8000
    python verify_api.py --all-scenarios --report-dir audits/verification_reports
    python verify_api.py --scenarios-override s0070,s0072

Requires:
    - DATABASE_URL env var
    - API running (default: https://api.coeqwal.org)
    - psycopg2, requests
"""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

try:
    import psycopg2
    import psycopg2.extras

    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

DEFAULT_API_URL = "https://api.coeqwal.org"

ABS_TOL = 0.01
REL_TOL = 0.001

# Add the repo root to sys.path so `etl.common` is importable when this
# script is run directly. See etl/common/__init__.py for the rationale.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from etl.common import resolve_active_scenarios  # noqa: E402
from etl.common.active_scenarios import ACTIVE_SCENARIOS  # noqa: E402

# Rebound inside main() when --scenarios-override is passed
ALL_SCENARIOS: frozenset[str] = ACTIVE_SCENARIOS


# Data Classes───


@dataclass
class Check:
    metric: str
    section: str
    entity: str
    db_value: Optional[float]
    api_value: Optional[float]
    abs_tol: float = ABS_TOL
    rel_tol: float = REL_TOL

    @property
    def status(self) -> str:
        if self.db_value is None and self.api_value is None:
            return "skip"
        if self.db_value is None or self.api_value is None:
            return "mismatch"
        if np.isnan(self.db_value) and np.isnan(self.api_value):
            return "pass"
        if np.isnan(self.db_value) or np.isnan(self.api_value):
            return "fail"
        if np.isclose(
            self.db_value, self.api_value, atol=self.abs_tol, rtol=self.rel_tol
        ):
            return "pass"
        return "fail"


@dataclass
class APIReport:
    scenario_id: str
    api_url: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    checks: List[Check] = field(default_factory=list)

    @property
    def summary(self) -> Dict[str, int]:
        statuses = [c.status for c in self.checks]
        return {
            "total": len(statuses),
            "pass": statuses.count("pass"),
            "fail": statuses.count("fail"),
            "mismatch": statuses.count("mismatch"),
            "skip": statuses.count("skip"),
        }

    def add(
        self,
        metric: str,
        section: str,
        entity: str,
        db_value: Optional[float],
        api_value: Optional[float],
    ):
        self.checks.append(
            Check(
                metric=metric,
                section=section,
                entity=entity,
                db_value=db_value,
                api_value=api_value,
            )
        )

    def to_dict(self) -> dict:
        checks = []
        for c in self.checks:
            d = asdict(c)
            d["status"] = c.status
            checks.append(d)
        return {
            "scenario_id": self.scenario_id,
            "api_url": self.api_url,
            "timestamp": self.timestamp,
            "summary": self.summary,
            "checks": checks,
        }

    def print_summary(self):
        s = self.summary
        print(f"\n{'=' * 70}")
        print(f"API VERIFICATION SUMMARY for {self.scenario_id}")
        print(f"{'=' * 70}")
        print(f"  API:           {self.api_url}")
        print(f"  Total checks:  {s['total']}")
        print(f"  PASS:          {s['pass']}")
        print(f"  FAIL:          {s['fail']}")
        print(f"  Mismatch:      {s['mismatch']}")
        print(f"  Skip:          {s['skip']}")
        failures = [c for c in self.checks if c.status == "fail"]
        if failures:
            print(f"\nFAILED ({len(failures)}):")
            for c in failures[:20]:
                print(
                    f"  [{c.section}] {c.entity}/{c.metric}: "
                    f"db={c.db_value}, api={c.api_value}"
                )


# Helpers────────


def _sf(val) -> Optional[float]:
    if val is None:
        return None
    return round(float(val), 6)


def connect_db():
    url = os.environ.get("DATABASE_URL")
    if not url or not HAS_PSYCOPG2:
        return None
    try:
        return psycopg2.connect(url)
    except Exception as e:
        log.error(f"DB connection failed: {e}")
        return None


def db_query(conn, sql: str, params: tuple = ()) -> List[dict]:
    if conn is None:
        return []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(row) for row in cur.fetchall()]


def api_get(base_url: str, path: str, params: dict = None) -> Optional[dict]:
    if not HAS_REQUESTS:
        log.error("requests not installed")
        return None
    url = f"{base_url}/api{path}"
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"API call failed: {url} -> {e}")
        return None


# Verify: Batch statistics


def verify_batch_storage(report: APIReport, conn, api_url: str, sid: str):
    section = "storage"
    log.info(f"  Checking {section}...")

    db_rows = db_query(
        conn,
        """
        SELECT re.short_code, rsm.water_month,
               rsm.storage_avg_taf, rsm.q50_taf
        FROM reservoir_storage_monthly rsm
        JOIN reservoir_entity re ON rsm.reservoir_entity_id = re.id
        JOIN reservoir_group_member rgm ON rgm.reservoir_entity_id = re.id
        JOIN reservoir_group rg ON rg.id = rgm.reservoir_group_id
        WHERE rsm.scenario_short_code = %s AND rg.short_code = 'major'
        ORDER BY re.short_code, rsm.water_month
    """,
        (sid,),
    )

    api_data = api_get(
        api_url, "/statistics/batch", {"scenarios": sid, "types": "storage"}
    )

    if not api_data or "storage" not in api_data:
        report.add("api_reachable", section, "batch", 1.0, 0.0)
        return

    api_storage = api_data["storage"].get(sid, {})
    api_reservoirs = api_storage.get("reservoirs", {})

    for row in db_rows:
        code = row["short_code"]
        wm = str(row["water_month"])
        db_avg = _sf(row["storage_avg_taf"])

        api_val = None
        if code in api_reservoirs:
            monthly = api_reservoirs[code].get("monthly_taf", {})
            if wm in monthly:
                api_val = _sf(monthly[wm].get("mean"))

        report.add(f"storage_avg_taf_m{wm}", section, code, db_avg, api_val)


def verify_batch_cws(report: APIReport, conn, api_url: str, sid: str):
    section = "cws"
    log.info(f"  Checking {section}...")

    db_rows = db_query(
        conn,
        """
        SELECT e.short_code, p.annual_delivery_avg_taf, p.reliability_pct
        FROM cws_aggregate_period_summary p
        JOIN cws_aggregate_entity e ON p.cws_aggregate_id = e.id
        WHERE p.scenario_short_code = %s AND e.is_active = TRUE
    """,
        (sid,),
    )

    api_data = api_get(api_url, "/statistics/batch", {"scenarios": sid, "types": "cws"})
    api_cws = {}
    if api_data and "cws" in api_data:
        period_data = api_data["cws"].get(sid, {}).get("period", {})
        api_cws = period_data.get("aggregates", {})

    for row in db_rows:
        code = row["short_code"]
        db_del = _sf(row["annual_delivery_avg_taf"])
        db_rel = _sf(row["reliability_pct"])

        api_del = _sf(api_cws.get(code, {}).get("annual_delivery_avg_taf"))
        api_rel = _sf(api_cws.get(code, {}).get("reliability_pct"))

        report.add("annual_delivery_avg_taf", section, code, db_del, api_del)
        report.add("reliability_pct", section, code, db_rel, api_rel)


def verify_batch_ag(report: APIReport, conn, api_url: str, sid: str):
    section = "ag"
    log.info(f"  Checking {section}...")

    db_rows = db_query(
        conn,
        """
        SELECT e.short_code, p.annual_delivery_avg_taf
        FROM ag_aggregate_period_summary p
        JOIN ag_aggregate_entity e ON p.ag_aggregate_id = e.id
        WHERE p.scenario_short_code = %s AND e.is_active = TRUE
    """,
        (sid,),
    )

    api_data = api_get(api_url, "/statistics/batch", {"scenarios": sid, "types": "ag"})
    api_ag = {}
    if api_data and "ag" in api_data:
        period_data = api_data["ag"].get(sid, {}).get("period", {})
        api_ag = period_data.get("aggregates", {})

    for row in db_rows:
        code = row["short_code"]
        db_del = _sf(row["annual_delivery_avg_taf"])
        api_del = _sf(api_ag.get(code, {}).get("annual_delivery_avg_taf"))
        report.add("annual_delivery_avg_taf", section, code, db_del, api_del)


def verify_delta(report: APIReport, conn, api_url: str, sid: str):
    """Verify delta data exists in DB. No API endpoint yet — DB-only check."""
    section = "delta"
    log.info(f"  Checking {section}...")

    monthly_count = db_query(
        conn,
        """
        SELECT COUNT(*) as cnt FROM delta_monthly
        WHERE scenario_short_code = %s
    """,
        (sid,),
    )
    cnt = monthly_count[0]["cnt"] if monthly_count else 0
    report.add("monthly_row_count", section, "all", None, float(cnt))

    period_rows = db_query(
        conn,
        """
        SELECT variable_code, category,
               summary_data->>'annual_avg_taf' as annual_avg_taf,
               summary_data->>'avg_km' as avg_km,
               summary_data->>'avg_ec' as avg_ec
        FROM delta_period_summary
        WHERE scenario_short_code = %s
        ORDER BY variable_code
    """,
        (sid,),
    )

    for row in period_rows:
        code = row["variable_code"]
        cat = row.get("category", "")
        if cat == "outflow":
            val = _sf(row.get("annual_avg_taf"))
            report.add("annual_avg_taf", section, code, None, val)
        elif cat == "x2":
            val = _sf(row.get("avg_km"))
            report.add("avg_km", section, code, None, val)
        elif cat and cat.startswith("salinity"):
            val = _sf(row.get("avg_ec"))
            report.add("avg_ec", section, code, None, val)

    if not period_rows:
        report.add("data_present", section, "all", 1.0, 0.0)


# Verify: Tiers─

TIER_CODES = [
    "CWS_DEL",
    "AG_REV",
    "ENV_FLOWS",
    "RES_STOR",
    "GW_STOR",
    "DELTA_ECO",
    "FW_DELTA_USES",
    "FW_EXP",
    "WRC_SALMON_AB",
]


def verify_tiers(report: APIReport, conn, api_url: str, sid: str):
    section = "tiers"
    log.info(f"  Checking {section}...")

    api_data = api_get(api_url, f"/tiers/scenarios/{sid}/tiers")
    if not api_data:
        report.add("api_reachable", section, "tiers_endpoint", 1.0, 0.0)
        return

    api_tiers = {}
    if isinstance(api_data, list):
        for t in api_data:
            api_tiers[t.get("tier_code")] = t
    elif isinstance(api_data, dict):
        api_tiers = api_data

    for tier_code in TIER_CODES:
        db_rows = db_query(
            conn,
            """
            SELECT single_tier_level, total_value,
                   norm_tier_1, norm_tier_2, norm_tier_3, norm_tier_4
            FROM tier_result
            WHERE scenario_short_code = %s
              AND tier_short_code = %s
              AND is_active = TRUE
        """,
            (sid, tier_code),
        )

        db_single = None
        db_total = None
        if db_rows:
            db_single = _sf(db_rows[0].get("single_tier_level"))
            db_total = _sf(db_rows[0].get("total_value"))

        api_tier = api_tiers.get(tier_code, {})
        api_single = _sf(api_tier.get("single_tier_level"))
        api_total = _sf(api_tier.get("total_value"))

        if db_single is not None:
            report.add("single_tier_level", section, tier_code, db_single, api_single)
        if db_total is not None:
            report.add("total_value", section, tier_code, db_total, api_total)

        api_score = _sf(api_tier.get("weighted_score"))
        if api_score is not None:
            report.add("weighted_score_present", section, tier_code, None, api_score)


# Verify: Env Flow Period


def verify_env_flow_period(report: APIReport, conn, api_url: str, sid: str):
    section = "env_flow_period"
    log.info(f"  Checking {section}...")

    db_rows = db_query(
        conn,
        """
        SELECT na.code, p.pearson_r, p.avg_pct_unimpaired, p.avg_pct_ff
        FROM env_flow_channel_period_summary p
        JOIN network_arc na ON p.network_arc_id = na.id
        WHERE p.scenario_short_code = %s
    """,
        (sid,),
    )

    api_data = api_get(api_url, f"/statistics/scenarios/{sid}/channels/period-summary")

    api_map: Dict[str, dict] = {}
    if api_data and isinstance(api_data, dict):
        for item in api_data.get("data", []):
            code = item.get("channel_code") or item.get("code")
            if code:
                api_map[code] = item

    for row in db_rows:
        code = row["code"]
        db_r = _sf(row["pearson_r"])
        db_unimp = _sf(row["avg_pct_unimpaired"])

        api_item = api_map.get(code, {})
        api_r = _sf(api_item.get("pearson_r"))
        api_unimp = _sf(api_item.get("avg_pct_unimpaired"))

        report.add("pearson_r", section, code, db_r, api_r)
        report.add("avg_pct_unimpaired", section, code, db_unimp, api_unimp)


# Main


def run_scenario(sid: str, api_url: str, report_dir: Optional[Path]) -> APIReport:
    report = APIReport(scenario_id=sid, api_url=api_url)
    conn = connect_db()
    if not conn:
        log.error("Cannot run API verification without DB connection")
        return report

    try:
        verify_batch_storage(report, conn, api_url, sid)
        verify_batch_cws(report, conn, api_url, sid)
        verify_batch_ag(report, conn, api_url, sid)
        verify_delta(report, conn, api_url, sid)
        verify_tiers(report, conn, api_url, sid)
        verify_env_flow_period(report, conn, api_url, sid)
    finally:
        conn.close()

    report.print_summary()

    if report_dir:
        report_dir.mkdir(parents=True, exist_ok=True)
        out_path = report_dir / f"{sid}_layer3.json"
        with open(out_path, "w") as f:
            json.dump(report.to_dict(), f, indent=2, default=str)
        log.info(f"Report written to {out_path}")

    return report


def render_scorecard(
    reports: List["APIReport"],
    json_paths: List[Path],
    file_obj=sys.stdout,
) -> None:
    """One-screen PASS / FAIL summary aggregated by section across scenarios.

    Each row is an API verifier section (storage, cws, ag, delta, tiers, env_flow).
    Counts sum across every scenario. Up to three failure examples are listed per
    failing section as `scenario/entity`. `Detail: <path>` points at the last
    JSON report written (if any).
    """
    if not reports:
        print("No reports to render.", file=file_obj)
        return

    scenarios = [r.scenario_id for r in reports]
    if len(scenarios) > 1:
        title = f"verify_api.py {scenarios[0]}..{scenarios[-1]} ({len(scenarios)} scenarios)"
    else:
        title = f"verify_api.py {scenarios[0]}"
    print(f"\n{title}", file=file_obj)
    print("=" * len(title), file=file_obj)

    # `fail` and `mismatch` both indicate a problem. Group them under one bucket.
    by_section: Dict[str, Dict[str, object]] = {}
    for r in reports:
        for c in r.checks:
            entry = by_section.setdefault(
                c.section, {"total": 0, "bad": 0, "bad_examples": []}
            )
            entry["total"] = int(entry["total"]) + 1  # type: ignore[arg-type]
            if c.status in ("fail", "mismatch"):
                entry["bad"] = int(entry["bad"]) + 1  # type: ignore[arg-type]
                examples: List[str] = entry["bad_examples"]  # type: ignore[assignment]
                if len(examples) < 3:
                    examples.append(f"{r.scenario_id}/{c.entity}")

    overall_pass = 0
    overall_total = 0
    for sec in sorted(by_section.keys()):
        e = by_section[sec]
        total = int(e["total"])
        n_bad = int(e["bad"])
        overall_total += 1
        if n_bad == 0:
            overall_pass += 1
            print(f"PASS {sec:<18} ({total} checks, 0 mismatches)", file=file_obj)
        else:
            examples = ", ".join(e["bad_examples"])  # type: ignore[arg-type]
            ellipsis = ", ..." if n_bad > 3 else ""
            print(
                f"FAIL {sec:<18} ({total} checks, {n_bad} mismatches: {examples}{ellipsis})",
                file=file_obj,
            )

    n_fail_sections = overall_total - overall_pass
    print(file=file_obj)
    if n_fail_sections == 0:
        print(f"Overall: {overall_pass}/{overall_total} sections PASS.", file=file_obj)
    else:
        print(
            f"Overall: {overall_pass}/{overall_total} sections PASS, {n_fail_sections} FAIL.",
            file=file_obj,
        )
    if json_paths:
        print(f"Detail: {json_paths[-1]}", file=file_obj)


def main():
    parser = argparse.ArgumentParser(
        description="Verify API responses against database for COEQWAL data explorer"
    )
    scenario_group = parser.add_mutually_exclusive_group()
    scenario_group.add_argument("--scenario", default=None)
    scenario_group.add_argument("--all-scenarios", action="store_true")
    scenario_group.add_argument(
        "--scenarios-override", nargs="*", default=[],
        help="Per-invocation replacement for ACTIVE_SCENARIOS when iterating. "
             "Comma/whitespace/newline separated. Use to pre-flight a scenario "
             "before flipping is_active=1. Logs a WARNING when active.",
    )
    parser.add_argument("--api-url", default=DEFAULT_API_URL)
    parser.add_argument("--report-dir", default=None)
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument(
        "--no-json",
        action="store_true",
        help="Skip the per-scenario JSON file write. Use for ad-hoc local runs.",
    )
    output_group.add_argument(
        "--json-stdout",
        action="store_true",
        help="Dump combined JSON to stdout instead of the scorecard. Useful for CI or piping to jq.",
    )
    args = parser.parse_args()

    global ALL_SCENARIOS  # noqa: PLW0603
    ALL_SCENARIOS = resolve_active_scenarios(args.scenarios_override)

    repo_root = Path(__file__).parent.parent.parent
    default_report_dir = repo_root / "audits" / "verification_reports"
    explicit_report_dir = Path(args.report_dir) if args.report_dir else default_report_dir
    report_dir_for_run: Optional[Path] = (
        None if (args.no_json or args.json_stdout) else explicit_report_dir
    )

    if args.all_scenarios or args.scenarios_override:
        scenarios = sorted(ALL_SCENARIOS)
    else:
        scenarios = [args.scenario or "s0020"]

    all_reports: List[APIReport] = []
    for sid in scenarios:
        log.info(f"\n{'=' * 70}")
        log.info(f"  API VERIFY: {sid}")
        log.info(f"{'=' * 70}")
        r = run_scenario(sid, args.api_url, report_dir_for_run)
        all_reports.append(r)

    if args.json_stdout:
        json.dump(
            {"scenarios": [r.to_dict() for r in all_reports]},
            sys.stdout,
            indent=2,
            default=str,
        )
        sys.stdout.write("\n")
    else:
        json_paths: List[Path] = []
        if report_dir_for_run:
            for r in all_reports:
                json_paths.append(report_dir_for_run / f"{r.scenario_id}_layer3.json")
        render_scorecard(all_reports, json_paths)

    total_fail = sum(r.summary["fail"] + r.summary["mismatch"] for r in all_reports)
    sys.exit(1 if total_fail > 0 else 0)


if __name__ == "__main__":
    main()
