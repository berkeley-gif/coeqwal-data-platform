"""
Verification status API endpoint for COEQWAL.

Serves verification report JSONs produced by the ETL verification scripts,
enabling a public data quality status page on the frontend.

Reports are stored as JSON files in audits/verification_reports/ by the
verify_all_sections.py (Layer 2) and verify_api.py (Layer 3) scripts.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, HTTPException, Query

log = logging.getLogger(__name__)

router = APIRouter(prefix="/api/verification", tags=["verification"])

_reports_dir: Optional[Path] = None

# Metrics from the statistics list that are not yet implemented
NOT_IMPLEMENTED_METRICS = [
    {
        "metric": "delta_outflow_volume",
        "section": "delta",
        "description": "Delta outflow volumes (NDO)",
        "variable": "NDO",
    },
    {
        "metric": "april_x2",
        "section": "delta_salinity",
        "description": "April X2 position (km from Golden Gate)",
        "variable": "X2_PRV_KM",
    },
    {
        "metric": "september_x2",
        "section": "delta_salinity",
        "description": "September X2 position (km from Golden Gate)",
        "variable": "X2_PRV_KM",
    },
    {
        "metric": "salinity_rock_slough",
        "section": "delta_salinity",
        "description": "Salinity at Rock Slough (RS_EC_MONTH)",
        "variable": "RS_EC_MONTH",
    },
    {
        "metric": "salinity_collinsville",
        "section": "delta_salinity",
        "description": "Salinity at Collinsville (CO_EC_MONTH)",
        "variable": "CO_EC_MONTH",
    },
    {
        "metric": "groundwater_level",
        "section": "groundwater",
        "description": "Groundwater level by WBA",
    },
    {
        "metric": "groundwater_storage_volume",
        "section": "groundwater",
        "description": "Groundwater storage volume by WBA",
    },
    {
        "metric": "groundwater_level_change",
        "section": "groundwater",
        "description": "Groundwater level change",
    },
    {
        "metric": "groundwater_storage_change_pct",
        "section": "groundwater",
        "description": "Groundwater storage change (% baseline)",
    },
    {
        "metric": "salmon_abundance",
        "section": "salmon",
        "description": "Max % change of 10-year rolling average spawner abundance (80th percentile)",
    },
]


def set_reports_dir(path: Path):
    global _reports_dir
    _reports_dir = path


def _get_reports_dir() -> Path:
    if _reports_dir and _reports_dir.exists():
        return _reports_dir
    fallback = Path(__file__).parent.parent.parent.parent / "audits" / "verification_reports"
    if fallback.exists():
        return fallback
    raise HTTPException(status_code=503, detail="Verification reports directory not found")


def _load_report(path: Path) -> Optional[dict]:
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Failed to load report {path}: {e}")
        return None


@router.get(
    "/status",
    summary="Get verification status for all scenarios",
    description=(
        "Returns the latest verification reports for all scenarios, "
        "including per-metric pass/fail status and not-yet-implemented metrics."
    ),
)
async def get_verification_status(
    scenario: Optional[str] = Query(
        None, description="Filter to a specific scenario ID"
    ),
) -> Dict[str, Any]:
    reports_dir = _get_reports_dir()

    layer2_reports = {}
    layer3_reports = {}

    for f in sorted(reports_dir.glob("*_layer2.json")):
        sid = f.stem.replace("_layer2", "")
        if scenario and sid != scenario:
            continue
        report = _load_report(f)
        if report:
            layer2_reports[sid] = report

    for f in sorted(reports_dir.glob("*_layer3.json")):
        sid = f.stem.replace("_layer3", "")
        if scenario and sid != scenario:
            continue
        report = _load_report(f)
        if report:
            layer3_reports[sid] = report

    all_scenarios = sorted(set(layer2_reports.keys()) | set(layer3_reports.keys()))

    scenario_summaries = []
    for sid in all_scenarios:
        l2 = layer2_reports.get(sid)
        l3 = layer3_reports.get(sid)
        scenario_summaries.append({
            "scenario_id": sid,
            "layer2": l2.get("summary") if l2 else None,
            "layer2_timestamp": l2.get("timestamp") if l2 else None,
            "layer2_db_connected": l2.get("db_connected") if l2 else None,
            "layer3": l3.get("summary") if l3 else None,
            "layer3_timestamp": l3.get("timestamp") if l3 else None,
        })

    return {
        "scenarios": scenario_summaries,
        "not_implemented": NOT_IMPLEMENTED_METRICS,
        "total_scenarios": len(all_scenarios),
    }


@router.get(
    "/status/{scenario_id}",
    summary="Get detailed verification for a specific scenario",
    description="Returns full Layer 2 and Layer 3 verification reports with all checks.",
)
async def get_scenario_verification(scenario_id: str) -> Dict[str, Any]:
    reports_dir = _get_reports_dir()

    l2_path = reports_dir / f"{scenario_id}_layer2.json"
    l3_path = reports_dir / f"{scenario_id}_layer3.json"

    l2 = _load_report(l2_path)
    l3 = _load_report(l3_path)

    if not l2 and not l3:
        raise HTTPException(
            status_code=404,
            detail=f"No verification reports found for {scenario_id}",
        )

    sections: Dict[str, List[dict]] = {}
    if l2 and "checks" in l2:
        for check in l2["checks"]:
            sec = check.get("section", "unknown")
            sections.setdefault(sec, []).append(check)

    return {
        "scenario_id": scenario_id,
        "layer2": l2,
        "layer3": l3,
        "sections": sections,
        "not_implemented": NOT_IMPLEMENTED_METRICS,
    }
