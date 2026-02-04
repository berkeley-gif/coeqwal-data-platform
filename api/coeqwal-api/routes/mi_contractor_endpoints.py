"""
M&I Contractor Statistics API Endpoints.

Provides statistics for SWP/CVP water contractors including:
- Monthly delivery statistics
- Monthly shortage statistics
- Period-of-record summary

Following the CWS aggregate pattern.
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

log = logging.getLogger(__name__)


def safe_float(val) -> Optional[float]:
    """Safely convert value to float, returning None for NULL.
    
    Important: Uses explicit None check to preserve zero values.
    """
    if val is None:
        return None
    return float(val)

router = APIRouter(prefix="/api/statistics", tags=["statistics"])

# Database pool - set by main.py at startup
_db_pool = None


def set_db_pool(pool):
    """Set the database connection pool."""
    global _db_pool
    _db_pool = pool


# =============================================================================
# LIST CONTRACTORS
# =============================================================================


@router.get(
    "/mi-contractors",
    summary="List M&I contractors",
    description="Returns available M&I contractor entities.",
)
async def list_mi_contractors():
    """List all M&I contractors."""
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                short_code,
                contractor_name,
                project,
                region,
                contractor_type,
                contract_amount_taf
            FROM mi_contractor
            WHERE is_active = TRUE
            ORDER BY short_code
            """
        )

    return {
        "contractors": [
            {
                "short_code": row["short_code"],
                "name": row["contractor_name"],
                "project": row["project"],
                "region": row["region"],
                "contractor_type": row["contractor_type"],
                "contract_amount_taf": float(row["contract_amount_taf"])
                if row["contract_amount_taf"]
                else None,
            }
            for row in rows
        ]
    }


# =============================================================================
# MONTHLY DELIVERY STATISTICS
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/mi-contractors/delivery-monthly",
    summary="Get monthly delivery statistics for M&I contractors",
)
async def get_mi_delivery_monthly(
    scenario_id: str,
    contractor: Optional[str] = Query(
        None, description="Comma-separated contractor codes to filter"
    ),
):
    """Get monthly delivery statistics for M&I contractors."""
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        # Build query with optional contractor filter
        query = """
            SELECT
                m.mi_contractor_code,
                c.contractor_name,
                m.water_month,
                m.delivery_avg_taf,
                m.delivery_cv,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.exc_p5, m.exc_p10, m.exc_p25, m.exc_p50, m.exc_p75, m.exc_p90, m.exc_p95,
                m.sample_count
            FROM mi_delivery_monthly m
            LEFT JOIN mi_contractor c ON m.mi_contractor_code = c.short_code
            WHERE m.scenario_short_code = $1
        """
        params = [scenario_id]

        if contractor:
            codes = [c.strip() for c in contractor.split(",")]
            query += " AND m.mi_contractor_code = ANY($2)"
            params.append(codes)

        query += " ORDER BY m.mi_contractor_code, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No delivery data found for scenario {scenario_id}",
        )

    # Group by contractor
    contractors = {}
    for row in rows:
        code = row["mi_contractor_code"]
        if code not in contractors:
            contractors[code] = {
                "label": row["contractor_name"],
                "monthly_delivery": {},
            }

        contractors[code]["monthly_delivery"][str(row["water_month"])] = {
            "avg_taf": float(row["delivery_avg_taf"]) if row["delivery_avg_taf"] is not None else None,
            "cv": float(row["delivery_cv"]) if row["delivery_cv"] is not None else None,
            "q0": float(row["q0"]) if row["q0"] is not None else None,
            "q10": float(row["q10"]) if row["q10"] is not None else None,
            "q30": float(row["q30"]) if row["q30"] is not None else None,
            "q50": float(row["q50"]) if row["q50"] is not None else None,
            "q70": float(row["q70"]) if row["q70"] is not None else None,
            "q90": float(row["q90"]) if row["q90"] is not None else None,
            "q100": float(row["q100"]) if row["q100"] is not None else None,
            "exc_p5": float(row["exc_p5"]) if row["exc_p5"] is not None else None,
            "exc_p10": float(row["exc_p10"]) if row["exc_p10"] is not None else None,
            "exc_p25": float(row["exc_p25"]) if row["exc_p25"] is not None else None,
            "exc_p50": float(row["exc_p50"]) if row["exc_p50"] is not None else None,
            "exc_p75": float(row["exc_p75"]) if row["exc_p75"] is not None else None,
            "exc_p90": float(row["exc_p90"]) if row["exc_p90"] is not None else None,
            "exc_p95": float(row["exc_p95"]) if row["exc_p95"] is not None else None,
            "sample_count": row["sample_count"],
        }

    return {"scenario_id": scenario_id, "contractors": contractors}


# =============================================================================
# MONTHLY SHORTAGE STATISTICS
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/mi-contractors/shortage-monthly",
    summary="Get monthly shortage statistics for M&I contractors",
)
async def get_mi_shortage_monthly(
    scenario_id: str,
    contractor: Optional[str] = Query(
        None, description="Comma-separated contractor codes to filter"
    ),
):
    """Get monthly shortage statistics for M&I contractors."""
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                m.mi_contractor_code,
                c.contractor_name,
                m.water_month,
                m.shortage_avg_taf,
                m.shortage_cv,
                m.shortage_frequency_pct,
                m.q0, m.q10, m.q30, m.q50, m.q70, m.q90, m.q100,
                m.sample_count
            FROM mi_shortage_monthly m
            LEFT JOIN mi_contractor c ON m.mi_contractor_code = c.short_code
            WHERE m.scenario_short_code = $1
        """
        params = [scenario_id]

        if contractor:
            codes = [c.strip() for c in contractor.split(",")]
            query += " AND m.mi_contractor_code = ANY($2)"
            params.append(codes)

        query += " ORDER BY m.mi_contractor_code, m.water_month"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No shortage data found for scenario {scenario_id}",
        )

    # Group by contractor
    contractors = {}
    for row in rows:
        code = row["mi_contractor_code"]
        if code not in contractors:
            contractors[code] = {
                "label": row["contractor_name"],
                "monthly_shortage": {},
            }

        contractors[code]["monthly_shortage"][str(row["water_month"])] = {
            "avg_taf": float(row["shortage_avg_taf"]) if row["shortage_avg_taf"] is not None else None,
            "cv": float(row["shortage_cv"]) if row["shortage_cv"] is not None else None,
            "frequency_pct": float(row["shortage_frequency_pct"])
            if row["shortage_frequency_pct"] is not None
            else None,
            "q0": float(row["q0"]) if row["q0"] is not None else None,
            "q10": float(row["q10"]) if row["q10"] is not None else None,
            "q30": float(row["q30"]) if row["q30"] is not None else None,
            "q50": float(row["q50"]) if row["q50"] is not None else None,
            "q70": float(row["q70"]) if row["q70"] is not None else None,
            "q90": float(row["q90"]) if row["q90"] is not None else None,
            "q100": float(row["q100"]) if row["q100"] is not None else None,
            "sample_count": row["sample_count"],
        }

    return {"scenario_id": scenario_id, "contractors": contractors}


# =============================================================================
# PERIOD SUMMARY
# =============================================================================


@router.get(
    "/scenarios/{scenario_id}/mi-contractors/period-summary",
    summary="Get period summary for M&I contractors",
)
async def get_mi_period_summary(
    scenario_id: str,
    contractor: Optional[str] = Query(
        None, description="Comma-separated contractor codes to filter"
    ),
):
    """Get period-of-record summary for M&I contractors."""
    if _db_pool is None:
        raise HTTPException(status_code=503, detail="Database not available")

    async with _db_pool.acquire() as conn:
        query = """
            SELECT
                p.mi_contractor_code,
                c.contractor_name,
                p.simulation_start_year,
                p.simulation_end_year,
                p.total_years,
                p.annual_delivery_avg_taf,
                p.annual_delivery_cv,
                p.delivery_exc_p5, p.delivery_exc_p10, p.delivery_exc_p25,
                p.delivery_exc_p50, p.delivery_exc_p75, p.delivery_exc_p90, p.delivery_exc_p95,
                p.annual_shortage_avg_taf,
                p.shortage_years_count,
                p.shortage_frequency_pct,
                p.shortage_exc_p5, p.shortage_exc_p10, p.shortage_exc_p25,
                p.shortage_exc_p50, p.shortage_exc_p75, p.shortage_exc_p90, p.shortage_exc_p95,
                p.reliability_pct
            FROM mi_contractor_period_summary p
            LEFT JOIN mi_contractor c ON p.mi_contractor_code = c.short_code
            WHERE p.scenario_short_code = $1
        """
        params = [scenario_id]

        if contractor:
            codes = [c.strip() for c in contractor.split(",")]
            query += " AND p.mi_contractor_code = ANY($2)"
            params.append(codes)

        query += " ORDER BY p.mi_contractor_code"

        rows = await conn.fetch(query, *params)

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No period summary found for scenario {scenario_id}",
        )

    contractors = {}
    for row in rows:
        code = row["mi_contractor_code"]
        contractors[code] = {
            "label": row["contractor_name"],
            "simulation_start_year": row["simulation_start_year"],
            "simulation_end_year": row["simulation_end_year"],
            "total_years": row["total_years"],
            "annual_delivery_avg_taf": float(row["annual_delivery_avg_taf"])
            if row["annual_delivery_avg_taf"] is not None
            else None,
            "annual_delivery_cv": float(row["annual_delivery_cv"])
            if row["annual_delivery_cv"] is not None
            else None,
            "delivery_exceedance": {
                "p5": float(row["delivery_exc_p5"]) if row["delivery_exc_p5"] is not None else None,
                "p10": float(row["delivery_exc_p10"]) if row["delivery_exc_p10"] is not None else None,
                "p25": float(row["delivery_exc_p25"]) if row["delivery_exc_p25"] is not None else None,
                "p50": float(row["delivery_exc_p50"]) if row["delivery_exc_p50"] is not None else None,
                "p75": float(row["delivery_exc_p75"]) if row["delivery_exc_p75"] is not None else None,
                "p90": float(row["delivery_exc_p90"]) if row["delivery_exc_p90"] is not None else None,
                "p95": float(row["delivery_exc_p95"]) if row["delivery_exc_p95"] is not None else None,
            },
            "annual_shortage_avg_taf": float(row["annual_shortage_avg_taf"])
            if row["annual_shortage_avg_taf"] is not None
            else None,
            "shortage_years_count": row["shortage_years_count"],
            "shortage_frequency_pct": float(row["shortage_frequency_pct"])
            if row["shortage_frequency_pct"] is not None
            else None,
            "shortage_exceedance": {
                "p5": float(row["shortage_exc_p5"]) if row["shortage_exc_p5"] is not None else None,
                "p10": float(row["shortage_exc_p10"]) if row["shortage_exc_p10"] is not None else None,
                "p25": float(row["shortage_exc_p25"]) if row["shortage_exc_p25"] is not None else None,
                "p50": float(row["shortage_exc_p50"]) if row["shortage_exc_p50"] is not None else None,
                "p75": float(row["shortage_exc_p75"]) if row["shortage_exc_p75"] is not None else None,
                "p90": float(row["shortage_exc_p90"]) if row["shortage_exc_p90"] is not None else None,
                "p95": float(row["shortage_exc_p95"]) if row["shortage_exc_p95"] is not None else None,
            },
            "reliability_pct": float(row["reliability_pct"])
            if row["reliability_pct"] is not None
            else None,
        }

    return {"scenario_id": scenario_id, "contractors": contractors}
