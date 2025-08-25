#!/usr/bin/env python
"""
DSS file name classifier (robust path matching + sensible fallbacks)

Expected layout (case-insensitive, works with relative paths):
  .../DSS/input/*.dss   -> SV candidates
  .../DSS/output/*.dss  -> CalSim output candidates

Tiered match per candidate list (case-insensitive, basename only):
  Tier 3 SV:  "_sv" in name
  Tier 3 Cal: "_dv" in name
  Tier 2 SV:  "statevar" OR "input" in name
  Tier 2 Cal: "out" OR "output" OR "results" in name

Return the FIRST candidate (original list order) that matches Tier 3,
else first Tier 2, else None.

Scenario ID:
  1. --scenario-id override (if provided)
  2. regex [A-Za-z0-9]\\d{4} from ZIP basename
  3. sanitized ZIP stem (non-alphanum -> _, lower, 12 chars)

Outputs env-style file:
  SCENARIO_ID=<id>
  SV_PATH=<rel-path-or-blank>
  CALSIM_OUTPUT_PATH=<rel-path-or-blank>
"""

import argparse
import os
import re
from typing import Optional, List, Tuple

RE_SCEN = re.compile(r'([A-Za-z0-9]\d{4})')

SV_TIER3 = "_sv"
SV_TIER2: Tuple[str, ...] = ("statevar", "input")

CAL_TIER3 = "_dv"
CAL_TIER2: Tuple[str, ...] = ("out", "output", "results")

# Basenames to ignore for CalSim output (lowercased)
GW_BASENAMES = ("cvgroundwaterbudget.dss", "cvgroundwaterout.dss")


def derive_scenario_id(zip_base: str, override: Optional[str]) -> str:
    if override:
        return override.lower()
    m = RE_SCEN.search(zip_base)
    if m:
        return m.group(1).lower()
    stem = os.path.splitext(zip_base)[0]
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', stem).lower()
    return safe[:12] or "scenario_fallback"


def pick_simple(candidates: List[str], tier3_token: str, tier2_tokens: Tuple[str, ...]) -> Optional[str]:
    """Pick first Tier 3 candidate by basename, else first Tier 2, else None."""
    if not candidates:
        return None
    # Tier 3
    for p in candidates:
        if tier3_token in os.path.basename(p).lower():
            return p
    # Tier 2
    for p in candidates:
        b = os.path.basename(p).lower()
        if any(tok in b for tok in tier2_tokens):
            return p
    return None


def _norm_for_match(path: str) -> str:
    """
    Normalize path for folder matching:
      - convert backslashes to forward slashes
      - lowercase
      - wrap with leading/trailing slash to stabilize substring checks
    """
    norm = path.replace("\\", "/").lstrip("./").lower()
    return f"/{norm}/"


def _filename_fallback(paths: List[str]) -> Tuple[List[str], List[str]]:
    """
    Fallback when DSS/input|output folders aren't found.
    Use filename heuristics only (still ignoring groundwater files for CalSim).
    """
    sv_cands: List[str] = []
    cal_cands: List[str] = []
    for p in paths:
        b = os.path.basename(p).lower()
        # Don't let groundwater files into CalSim outputs
        if b not in GW_BASENAMES:
            # Heuristic: if it "looks like" an output (dv/out/results), consider it CalSim
            if (CAL_TIER3 in b) or any(tok in b for tok in CAL_TIER2):
                cal_cands.append(p)
        # Heuristic for SV
        if (SV_TIER3 in b) or any(tok in b for tok in SV_TIER2):
            sv_cands.append(p)
    return sv_cands, cal_cands


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--zip-base", required=True, help="ZIP basename (no path)")
    ap.add_argument("--paths-file", required=True, help="newline-delimited relative DSS paths")
    ap.add_argument("--scenario-id", default=None, help="override scenario id")
    ap.add_argument("--out-env", required=True, help="output env file path")
    args = ap.parse_args()

    with open(args.paths_file) as f:
        # Preserve original order (used as tie-breaker for 'first' match)
        paths = [ln.strip() for ln in f if ln.strip()]

    scen = derive_scenario_id(args.zip_base, args.scenario_id)

    # Primary selection based on folder layout, robust to case/relative paths/backslashes
    sv_candidates: List[str] = []
    cal_candidates: List[str] = []
    for p in paths:
        slug = _norm_for_match(p)  # e.g. "/dss/input/foo.dss/"
        b = os.path.basename(p).lower()
        if "/dss/input/" in slug:
            sv_candidates.append(p)
        elif "/dss/output/" in slug:
            if b not in GW_BASENAMES:
                cal_candidates.append(p)

    # If we didn't find anything via folder structure, fall back to filename heuristics
    if not sv_candidates and not cal_candidates:
        fb_sv, fb_cal = _filename_fallback(paths)
        if not sv_candidates:
            sv_candidates = fb_sv
        if not cal_candidates:
            cal_candidates = fb_cal

    sv_path = pick_simple(sv_candidates, SV_TIER3, SV_TIER2)
    calsim_output_path = pick_simple(cal_candidates, CAL_TIER3, CAL_TIER2)

    with open(args.out_env, "w") as out:
        out.write(f"SCENARIO_ID={scen}\n")
        out.write(f"SV_PATH={sv_path or ''}\n")
        out.write(f"CALSIM_OUTPUT_PATH={calsim_output_path or ''}\n")


if __name__ == "__main__":
    main()