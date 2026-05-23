"""staging_inventory.py - Parse the tier data staging CSVs into per-tier location inventories.

Used by `audit_tier_location_geometry.py`, `sync_tier_locations_from_staging.py`,
and `diff_tier_locations.py`.

A staging CSV is the flat output of
[`stage_tier_results.py`](./scripts/stage_tier_results.py), with one of these shapes:

  - wide multi-location: column 0 carries the scenario label and every
    remaining column header is a location_id
    (CWS_DEL, RES_STOR, GW_STOR, AG_REV-wide, ENV_FLOWS)
  - single fixed location: the CSV's presence is the only signal
    (DELTA_ECO -> DETAW, WRC_SALMON_AB -> SAC299, FW_DELTA_USES, FW_EXP)
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


_RES_STOR_COL_RE = re.compile(r"^S_(?P<short>[A-Z0-9_]+?)_Storage_Tier$")


def parse_res_stor_column(column: str) -> str:
    """`S_SHSTA_Storage_Tier` -> `SHSTA`. Returns the column verbatim if it
    doesn't match the expected pattern.
    """
    m = _RES_STOR_COL_RE.match(column)
    return m.group("short") if m else column


def convert_wba_id_to_mapbox_format(wba_col: str) -> str:
    """`WBA2` -> `02`, `WBA7N` -> `07N`, `WBA10` -> `10`, `DETAW` -> `DETAW`."""
    if wba_col == "DETAW":
        return "DETAW"
    if wba_col.startswith("WBA"):
        suffix = wba_col[3:]
        if suffix and suffix[0].isdigit():
            if len(suffix) == 1 or (len(suffix) == 2 and suffix[1] in "NS"):
                return "0" + suffix
        return suffix
    return wba_col


# Mapping of tier_short_code -> location_type used by every consumer. Keep
# in sync with the CHECK constraint on tier_location.location_type and the
# `LOCATION_ENTITY_MAP` registry in `etl/common/tier_location_entities.py`.
TIER_LOCATION_TYPE: Dict[str, str] = {
    "ENV_FLOWS": "network_node",
    "RES_STOR": "reservoir",
    "GW_STOR": "wba",
    "CWS_DEL": "demand_unit",
    "AG_REV": "demand_unit",
    "DELTA_ECO": "wba",
    "FW_DELTA_USES": "compliance_station",
    "FW_EXP": "network_node",
    "WRC_SALMON_AB": "network_node",
}


@dataclass
class StagingMember:
    """One location_id discovered in a staging CSV, plus its display order."""

    location_id: str
    display_order: int


@dataclass
class StagingInventory:
    """Per-tier extraction: which location_ids the staging CSV declares."""

    tier: str
    location_type: str
    members: List[StagingMember] = field(default_factory=list)
    source_files: List[str] = field(default_factory=list)

    @property
    def ids(self) -> List[str]:
        return [m.location_id for m in self.members]


# Single-location tiers: the staging CSV's presence is the only signal we need.
SINGLE_LOCATION_DEFAULTS: Dict[str, List[Tuple[str, int]]] = {
    "DELTA_ECO": [("DETAW", 1)],
    "FW_DELTA_USES": [("EM", 1), ("JP", 2)],
    "FW_EXP": [("CAA003", 1), ("DMC000", 2)],
    "WRC_SALMON_AB": [("SAC299", 1)],
}


def _dedupe(members: Iterable[StagingMember]) -> List[StagingMember]:
    """First occurrence wins; preserves display_order from earliest sighting."""
    seen: Dict[str, StagingMember] = {}
    for m in members:
        if m.location_id and m.location_id not in seen:
            seen[m.location_id] = m
    return list(seen.values())


def _env_flows_ids(staging_dir: Path) -> Tuple[List[StagingMember], List[str]]:
    """Parse ENV_FLOWS staging CSV(s). Same wide format as RES_STOR /
    GW_STOR / CWS_DEL (column 0 = scenario, remaining columns =
    network_node short codes like `AMR004`, `SAC289`). Lives in its own
    function only because ENV_FLOWS additionally supports the per-climate
    split filenames `ENV_FLOWS_{historical,cc50,cc95}.csv` produced by
    `scripts/stage_tier_results.py`.
    """
    import pandas as pd

    members: List[StagingMember] = []
    sources: List[str] = []
    paths: List[Path] = []
    legacy = staging_dir / "ENV_FLOWS.csv"
    if legacy.exists():
        paths.append(legacy)
    paths.extend(sorted(staging_dir.glob("ENV_FLOWS_*.csv")))
    order = 1
    for path in paths:
        df = pd.read_csv(path)
        for col in df.columns[1:]:
            members.append(StagingMember(location_id=str(col).strip(), display_order=order))
            order += 1
        sources.append(path.name)
    return _dedupe(members), sources


def _columns_with_order(
    staging_dir: Path,
    filename: str,
    skip_first: bool,
    transform=lambda c: c,
) -> Tuple[List[StagingMember], List[str]]:
    import pandas as pd

    path = staging_dir / filename
    if not path.exists():
        return [], []
    df = pd.read_csv(path)
    cols = list(df.columns[1:] if skip_first else df.columns)
    members = [
        StagingMember(location_id=transform(c), display_order=i + 1)
        for i, c in enumerate(cols)
    ]
    return _dedupe(members), [path.name]


def _ag_rev_ids(staging_dir: Path) -> Tuple[List[StagingMember], List[str]]:
    import pandas as pd

    path = staging_dir / "AG_REV.csv"
    if not path.exists():
        return [], []
    df = pd.read_csv(path)
    if "region" in df.columns and "tier" in df.columns:
        order_map: Dict[str, int] = {}
        for region in df["region"].dropna():
            r = str(region).strip()
            if r and r not in order_map:
                order_map[r] = len(order_map) + 1
        members = [StagingMember(location_id=k, display_order=v) for k, v in order_map.items()]
        return _dedupe(members), [path.name]
    cols = list(df.columns[1:])
    members = [StagingMember(location_id=c, display_order=i + 1) for i, c in enumerate(cols)]
    return _dedupe(members), [path.name]


def build_inventory(staging_dir: Path) -> Dict[str, StagingInventory]:
    """Return {tier_short_code: StagingInventory}. Skips tiers whose CSV is absent."""
    out: Dict[str, StagingInventory] = {}

    def _emit(tier: str, members: List[StagingMember], sources: List[str]) -> None:
        if not members:
            return
        out[tier] = StagingInventory(
            tier=tier,
            location_type=TIER_LOCATION_TYPE[tier],
            members=members,
            source_files=sources,
        )

    members, sources = _env_flows_ids(staging_dir)
    _emit("ENV_FLOWS", members, sources)

    members, sources = _columns_with_order(staging_dir, "RES_STOR.csv", skip_first=True, transform=parse_res_stor_column)
    _emit("RES_STOR", members, sources)

    members, sources = _columns_with_order(staging_dir, "GW_STOR.csv", skip_first=True, transform=convert_wba_id_to_mapbox_format)
    _emit("GW_STOR", members, sources)

    members, sources = _columns_with_order(staging_dir, "CWS_DEL.csv", skip_first=True)
    _emit("CWS_DEL", members, sources)

    members, sources = _ag_rev_ids(staging_dir)
    _emit("AG_REV", members, sources)

    for tier, defaults in SINGLE_LOCATION_DEFAULTS.items():
        candidates = [staging_dir / f"{tier}.csv"]
        if not any(p.exists() for p in candidates):
            continue
        members = [StagingMember(location_id=lid, display_order=order) for lid, order in defaults]
        _emit(tier, members, [f"{tier}.csv"])

    return out


def inventory_summary(inventory: Dict[str, StagingInventory]) -> List[str]:
    """Single-line summary per tier, for printing in scripts."""
    lines: List[str] = []
    width = max((len(t) for t in inventory), default=0)
    for tier in sorted(inventory):
        inv = inventory[tier]
        lines.append(
            f"  {tier.ljust(width)}  [{inv.location_type}]  "
            f"{len(inv.members)} ids  ({', '.join(inv.source_files)})"
        )
    return lines


__all__ = [
    "StagingInventory",
    "StagingMember",
    "TIER_LOCATION_TYPE",
    "SINGLE_LOCATION_DEFAULTS",
    "build_inventory",
    "convert_wba_id_to_mapbox_format",
    "inventory_summary",
    "parse_res_stor_column",
]
