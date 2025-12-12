"""
Tier API endpoints for COEQWAL interpretive framework.

Provides tier definitions and scenario tier data for outcome visualization.

Tier System:
- Tier 1 (Green): Best outcomes
- Tier 2 (Blue): Good outcomes
- Tier 3 (Orange): Moderate concern
- Tier 4 (Red): Significant concern

Two tier types:
- multi_value: Distribution across locations (e.g., 70 tier-1, 30 tier-2, etc.)
- single_value: Single overall tier level (1-4)
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional, Any
import asyncpg
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/tiers", tags=["tiers"])

# =============================================================================
# PYDANTIC MODELS
# =============================================================================

class TierDefinition(BaseModel):
    """Definition of a tier indicator"""
    short_code: str = Field(..., description="Unique identifier (e.g., 'AG_REV', 'CWS_DEL')")
    name: str = Field(..., description="Display name (e.g., 'Agricultural revenue')")
    description: str = Field(..., description="Detailed description for tooltips")
    tier_type: str = Field(..., description="'multi_value' or 'single_value'")
    tier_count: int = Field(..., description="Number of tier levels (usually 4)")
    is_active: bool = Field(..., description="Whether this tier is currently active")

class TierData(BaseModel):
    """Single tier level data point"""
    tier: str = Field(..., description="Tier identifier: 'tier1', 'tier2', 'tier3', 'tier4'")
    value: Optional[int] = Field(None, description="Raw count of locations at this tier")
    normalized: Optional[float] = Field(None, description="Normalized value (0.0-1.0)")

class MultiValueTierResult(BaseModel):
    """Result for multi-value tier (distribution across locations)"""
    scenario: str = Field(..., description="Scenario ID (e.g., 's0020')")
    tier_code: str = Field(..., description="Tier indicator code")
    tier_type: str = Field("multi_value", description="Always 'multi_value'")
    data: List[TierData] = Field(..., description="Tier distribution data")
    total_value: int = Field(..., description="Total number of locations")

class SingleValueTierResult(BaseModel):
    """Result for single-value tier (one overall tier level)"""
    scenario: str = Field(..., description="Scenario ID (e.g., 's0020')")
    tier_code: str = Field(..., description="Tier indicator code")
    tier_type: str = Field("single_value", description="Always 'single_value'")
    single_tier_level: int = Field(..., ge=1, le=4, description="Overall tier level (1-4)")


# Database connection dependency (set by main.py)
db_pool = None

def set_db_pool(pool):
    global db_pool
    db_pool = pool

async def get_db():
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database not available")
    async with db_pool.acquire() as connection:
        yield connection


def calculate_gini(t1_pct: float, t2_pct: float, t3_pct: float, t4_pct: float) -> float:
    """
    Gini coefficient measuring inequality in tier distribution.
    0.0 = all locations at same tier (equitable)
    ~1.0 = maximum polarization (inequitable)
    """
    tiers = [1, 2, 3, 4]
    pcts = [t1_pct or 0, t2_pct or 0, t3_pct or 0, t4_pct or 0]
    
    # Weighted mean tier level
    mean = sum(t * p for t, p in zip(tiers, pcts))
    if mean == 0:
        return 0.0
    
    # Mean absolute difference between all pairs
    total_diff = 0.0
    for i, (t_i, p_i) in enumerate(zip(tiers, pcts)):
        for j, (t_j, p_j) in enumerate(zip(tiers, pcts)):
            total_diff += abs(t_i - t_j) * p_i * p_j
    
    gini = total_diff / (2 * mean)
    return round(gini, 3)


def get_best_tier_present(t1_pct: float, t2_pct: float, t3_pct: float, t4_pct: float) -> int:
    """Return the best (lowest numbered) tier with non-zero percentage."""
    if (t1_pct or 0) > 0:
        return 1
    if (t2_pct or 0) > 0:
        return 2
    if (t3_pct or 0) > 0:
        return 3
    if (t4_pct or 0) > 0:
        return 4
    return 1  # fallback


def get_worst_tier_present(t1_pct: float, t2_pct: float, t3_pct: float, t4_pct: float) -> int:
    """Return the worst (highest numbered) tier with non-zero percentage."""
    if (t4_pct or 0) > 0:
        return 4
    if (t3_pct or 0) > 0:
        return 3
    if (t2_pct or 0) > 0:
        return 2
    if (t1_pct or 0) > 0:
        return 1
    return 4  # fallback


def calculate_tier_scores(norm_1: float, norm_2: float, norm_3: float, norm_4: float) -> dict:
    """
    Calculate comprehensive scores for multi-value tiers.
    
    Returns:
    - weighted_score: 1.0 (best) to 4.0 (worst) - for sorting scenarios
    - normalized_score: 0.0 to 1.0 - Y-axis for parallel plot (higher = better)
    - gini: 0.0 to ~1.0 - equity indicator (lower = more equitable)
    - band_upper: 0.0 to 1.0 - top edge of spread band on parallel plot
    - band_lower: 0.0 to 1.0 - bottom edge of spread band on parallel plot
    """
    n1 = norm_1 or 0.0
    n2 = norm_2 or 0.0
    n3 = norm_3 or 0.0
    n4 = norm_4 or 0.0
    
    total_pct = n1 + n2 + n3 + n4
    
    # Handle missing/zero data
    if total_pct == 0:
        return {
            "weighted_score": None,
            "normalized_score": None,
            "gini": None,
            "band_upper": None,
            "band_lower": None
        }
    
    # 1. Weighted score (1-4 scale, lower = better)
    weighted_sum = (1 * n1) + (2 * n2) + (3 * n3) + (4 * n4)
    weighted_score = round(weighted_sum / total_pct, 3)
    
    # 2. Normalized score (0-1 scale, higher = better)
    # When weighted_score = 1.0 → normalized = 1.0 (best)
    # When weighted_score = 4.0 → normalized = 0.0 (worst)
    normalized_score = round((4.0 - weighted_score) / 3.0, 3)
    
    # 3. Gini coefficient (0 = equitable, ~1 = polarized)
    gini = calculate_gini(n1, n2, n3, n4)
    
    # 4. Band upper (best tier present, normalized to 0-1)
    best_tier = get_best_tier_present(n1, n2, n3, n4)
    band_upper = round((4.0 - best_tier) / 3.0, 3)
    
    # 5. Band lower (worst tier present, normalized to 0-1)
    worst_tier = get_worst_tier_present(n1, n2, n3, n4)
    band_lower = round((4.0 - worst_tier) / 3.0, 3)
    
    return {
        "weighted_score": weighted_score,
        "normalized_score": normalized_score,
        "gini": gini,
        "band_upper": band_upper,
        "band_lower": band_lower
    }

@router.get("/definitions", summary="Get tier descriptions")
async def get_tier_definitions(
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, str]:
    """
    Get tier definitions as a simple short_code → description mapping.
    
    **Use case:** Populate tooltips and help text in the UI.
    
    **Response format:**
    ```json
    {
      "AG_REV": "Impact on agricultural production and revenue",
      "CWS_DEL": "Reliability of deliveries to community water systems",
      ...
    }
    ```
    """
    try:
        query = """
        SELECT short_code, name, description 
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY short_code
        """
        
        rows = await connection.fetch(query)
        
        # Return as {short_code: description} for frontend compatibility
        return {
            row['short_code']: row['description'] or row['name']
            for row in rows
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/list", summary="List all tier indicators", response_model=List[TierDefinition])
async def get_all_tier_definitions(
    connection: asyncpg.Connection = Depends(get_db)
) -> List[TierDefinition]:
    """
    Get complete list of tier indicator definitions.
    
    **Use case:** Build tier selection UI, understand available indicators.
    
    Returns full metadata for each tier including:
    - `short_code`: Unique identifier for API calls
    - `name`: Human-readable display name
    - `tier_type`: 'multi_value' (distribution) or 'single_value' (overall score)
    - `tier_count`: Number of locations (for multi_value) or 1 (for single_value)
    """
    try:
        query = """
        SELECT short_code, name, description, tier_type, tier_count, is_active
        FROM tier_definition 
        WHERE is_active = TRUE 
        ORDER BY tier_type DESC, short_code
        """
        
        rows = await connection.fetch(query)
        
        return [
            TierDefinition(
                short_code=row['short_code'],
                name=row['name'],
                description=row['description'] or '',
                tier_type=row['tier_type'],
                tier_count=row['tier_count'],
                is_active=row['is_active']
            )
            for row in rows
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/scenarios/{scenario_id}/tiers/{tier_short_code}", summary="Get single tier for scenario")
async def get_scenario_tier_data(
    scenario_id: str,
    tier_short_code: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get specific tier indicator data for a scenario.
    
    **Example:** `GET /api/tiers/scenarios/s0020/tiers/AG_REV`
    
    **Multi-value response (AG_REV, CWS_DEL, etc.):**
    ```json
    {
      "scenario": "s0020",
      "tier_code": "AG_REV",
      "name": "Agricultural revenue",
      "tier_type": "multi_value",
      "weighted_score": 1.778,
      "data": [
        {"tier": "tier1", "value": 70, "normalized": 0.53},
        {"tier": "tier2", "value": 35, "normalized": 0.265},
        {"tier": "tier3", "value": 13, "normalized": 0.098},
        {"tier": "tier4", "value": 14, "normalized": 0.106}
      ],
      "total_value": 132
    }
    ```
    
    `weighted_score` formula: `(1×tier1% + 2×tier2% + 3×tier3% + 4×tier4%) / total%`
    Range: 1.0 (best) to 4.0 (worst) - use for sorting/comparison
    
    **Single-value response (DELTA_ECO, FW_EXP, etc.):**
    ```json
    {
      "scenario": "s0020",
      "tier_code": "DELTA_ECO",
      "name": "Delta ecology",
      "tier_type": "single_value",
      "single_tier_level": 3
    }
    ```
    """
    try:
        query = """
        SELECT 
            tr.scenario_short_code,
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = $1 
        AND tr.tier_short_code = $2
        AND tr.is_active = TRUE
        """
        
        row = await connection.fetchrow(query, scenario_id, tier_short_code)
        
        if not row:
            raise HTTPException(
                status_code=404, 
                detail=f"Tier data not found for scenario {scenario_id}, tier {tier_short_code}"
            )
        
        # Return different format based on tier_type
        if row['tier_type'] == 'multi_value':
            norm_1 = float(row['norm_tier_1']) if row['norm_tier_1'] else 0.0
            norm_2 = float(row['norm_tier_2']) if row['norm_tier_2'] else 0.0
            norm_3 = float(row['norm_tier_3']) if row['norm_tier_3'] else 0.0
            norm_4 = float(row['norm_tier_4']) if row['norm_tier_4'] else 0.0
            
            scores = calculate_tier_scores(norm_1, norm_2, norm_3, norm_4)
            
            return {
                "scenario": row['scenario_short_code'],
                "tier_code": row['tier_short_code'],
                "name": row['name'],
                "tier_type": "multi_value",
                "weighted_score": scores["weighted_score"],
                "normalized_score": scores["normalized_score"],
                "gini": scores["gini"],
                "band_upper": scores["band_upper"],
                "band_lower": scores["band_lower"],
                "data": [
                    {"tier": "tier1", "value": row['tier_1_value'], "normalized": norm_1},
                    {"tier": "tier2", "value": row['tier_2_value'], "normalized": norm_2},
                    {"tier": "tier3", "value": row['tier_3_value'], "normalized": norm_3},
                    {"tier": "tier4", "value": row['tier_4_value'], "normalized": norm_4}
                ],
                "total_value": row['total_value']
            }
        else:
            # Single value tier - gini=0, band_upper=band_lower=normalized_score
            level = row['single_tier_level'] or 0
            weighted = float(level) if level else 0.0
            normalized = round((4.0 - weighted) / 3.0, 3) if level else 0.0
            return {
                "scenario": row['scenario_short_code'],
                "tier_code": row['tier_short_code'], 
                "name": row['name'],
                "tier_type": "single_value",
                "weighted_score": weighted,
                "normalized_score": normalized,
                "gini": 0.0,
                "band_upper": normalized,
                "band_lower": normalized,
                "single_tier_level": level
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@router.get("/scenarios/{scenario_id}/tiers", summary="Get all tiers for scenario")
async def get_all_scenario_tiers(
    scenario_id: str,
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get all tier indicator data for a scenario in a single request.
    
    **Use case:** Load all data for scenario comparison charts.
    
    **Example:** `GET /api/tiers/scenarios/s0020/tiers`
    
    Returns a dictionary of all tier indicators keyed by short_code:
    ```json
    {
      "scenario": "s0020",
      "tiers": {
        "AG_REV": { "name": "...", "type": "multi_value", "weighted_score": 1.78, "data": [...], "total": 132 },
        "CWS_DEL": { "name": "...", "type": "multi_value", "weighted_score": 1.12, "data": [...], "total": 91 },
        "DELTA_ECO": { "name": "...", "type": "single_value", "weighted_score": 3.0, "level": 3 },
        ...
      }
    }
    ```
    
    `weighted_score` is 1.0-4.0 for all tiers (lower is better). Use for sorting/comparison.
    """
    try:
        query = """
        SELECT 
            tr.tier_short_code,
            td.name,
            td.tier_type,
            tr.tier_1_value,
            tr.tier_2_value,
            tr.tier_3_value,
            tr.tier_4_value,
            tr.norm_tier_1,
            tr.norm_tier_2,
            tr.norm_tier_3,
            tr.norm_tier_4,
            tr.total_value,
            tr.single_tier_level
        FROM tier_result tr
        JOIN tier_definition td ON tr.tier_short_code = td.short_code
        WHERE tr.scenario_short_code = $1
        AND tr.is_active = TRUE
        ORDER BY td.tier_type DESC, tr.tier_short_code
        """
        
        rows = await connection.fetch(query, scenario_id)
        
        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"No tier data found for scenario {scenario_id}"
            )
        
        tiers = {}
        
        for row in rows:
            tier_code = row['tier_short_code']
            
            if row['tier_type'] == 'multi_value':
                norm_1 = float(row['norm_tier_1']) if row['norm_tier_1'] else 0.0
                norm_2 = float(row['norm_tier_2']) if row['norm_tier_2'] else 0.0
                norm_3 = float(row['norm_tier_3']) if row['norm_tier_3'] else 0.0
                norm_4 = float(row['norm_tier_4']) if row['norm_tier_4'] else 0.0
                
                scores = calculate_tier_scores(norm_1, norm_2, norm_3, norm_4)
                
                tiers[tier_code] = {
                    "name": row['name'],
                    "type": "multi_value",
                    "weighted_score": scores["weighted_score"],
                    "normalized_score": scores["normalized_score"],
                    "gini": scores["gini"],
                    "band_upper": scores["band_upper"],
                    "band_lower": scores["band_lower"],
                    "data": [
                        {"tier": "tier1", "value": row['tier_1_value'], "normalized": norm_1},
                        {"tier": "tier2", "value": row['tier_2_value'], "normalized": norm_2},
                        {"tier": "tier3", "value": row['tier_3_value'], "normalized": norm_3},
                        {"tier": "tier4", "value": row['tier_4_value'], "normalized": norm_4}
                    ],
                    "total": row['total_value']
                }
            else:
                level = row['single_tier_level'] or 0
                weighted = float(level) if level else 0.0
                normalized = round((4.0 - weighted) / 3.0, 3) if level else 0.0
                tiers[tier_code] = {
                    "name": row['name'],
                    "type": "single_value", 
                    "weighted_score": weighted,
                    "normalized_score": normalized,
                    "gini": 0.0,
                    "band_upper": normalized,
                    "band_lower": normalized,
                    "level": level
                }
        
        return {
            "scenario": scenario_id,
            "tiers": tiers
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# =============================================================================
# DISCOVERY ENDPOINTS (For Scientists/Researchers)
# =============================================================================

@router.get("/scenarios", summary="List available scenarios")
async def get_available_scenarios(
    connection: asyncpg.Connection = Depends(get_db)
) -> Dict[str, Any]:
    """
    Discover which scenarios have tier data available.
    
    **Use case:** Researchers can see what data exists before querying.
    
    **Example:** `GET /api/tiers/scenarios`
    
    **Response:**
    ```json
    {
      "scenarios": [
        {
          "scenario_id": "s0020",
          "tiers": ["AG_REV", "CWS_DEL", "DELTA_ECO", ...],
          "tier_count": 9
        },
        ...
      ],
      "total": 8
    }
    ```
    """
    try:
        query = """
        SELECT 
            tr.scenario_short_code,
            array_agg(DISTINCT tr.tier_short_code ORDER BY tr.tier_short_code) as tiers,
            COUNT(DISTINCT tr.tier_short_code) as tier_count
        FROM tier_result tr
        WHERE tr.is_active = TRUE
        GROUP BY tr.scenario_short_code
        ORDER BY tr.scenario_short_code
        """
        rows = await connection.fetch(query)
        
        scenarios = [
            {
                "scenario_id": row['scenario_short_code'],
                "tiers": list(row['tiers']),
                "tier_count": row['tier_count']
            }
            for row in rows
        ]
        
        return {
            "scenarios": scenarios,
            "total": len(scenarios)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
