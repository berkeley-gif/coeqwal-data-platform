# data_in_depth

ETL domain for the CalSim **trend-report variable export** — a wide DSS dump of
per-variable, per-scenario time series that we decompose and (eventually) turn
into SQL insert scripts for statistics-shaped tables.

This is a **standalone pipeline**, deliberately separate from
`etl/statistics/run_all.py`. It is driven by an external CSV on its own cadence
(when the file is refreshed), not by the per-scenario DSS extraction that
`run_all.py` orchestrates. Nothing here runs automatically.

## Status

| Piece | State |
|---|---|
| CSV parser + subset accessor (`scripts/`) | ✅ built & validated |
| Target table DDL (subject, value) + WYT table | ✅ written (`database/scripts/sql/`) |
| WYT extractor (`extract_wyt.py`) | ✅ built |
| Reservoir-storage extractor (`extract_reservoir_storage.py`) | ✅ built |
| River-flow, delta-salinity, CWS, ag, groundwater, salmon extractors | ✅ built (see `open_issues.md`) |
| System-deliveries seeder + extractor (`extract_system_deliveries.py`) | ✅ built (25 metric subjects, no aggregation) |

Known gaps are tracked in [`open_issues.md`](open_issues.md).

## Source data

`data/raw/trend_report_variables_v5.csv` (~566 MB, **not** committed — lives
under the gitignored `data/` tree).

It is a **DSS-pathname export**, not a normal CSV. The first 7 rows are the DSS
pathname parts; column 0 is a label column:

| row (0-based) | DSS part | example | use |
|---|---|---|---|
| 0 | A-part | `CALSIM` | dropped |
| 1 | **B-part** | `AWOANN_64_XADV_s0002` | **column key** → `variable` + `scenario` |
| 2 | C-part | `ANNUAL-APPLIED-WATER` / `FLOW` | kept as metadata |
| 3 | D-part | `1MON` | dropped |
| 4 | E-part | `L2020A` | dropped |
| 5 | F-part | `PER-AVER` | dropped |
| 6 | Units | `TAF` / `CFS` / `KM` / `UMHOS/CM` | kept (disambiguator) |
| 7+ | data | col 0 = date (`1921-10-31`), rest = values | monthly time series |

Shape as of `v5`: **46,175 data columns**, **250 variables × 116 scenarios**,
~**1,200 monthly rows** (1921-10-31 →). **s0002 (the first scenario) is excluded
from all extracts by default**, so extractions see **115 scenarios** (override with
`TrendReport.load(exclude_scenarios=None)`). Two encoded dimensions live in each
B-part: `AWOANN_64_XADV_s0002` → variable `AWOANN_64_XADV` + scenario `s0002`
(the variable itself contains underscores; the scenario is the trailing
`_s<digits>`).

**CFS/TAF duplication:** flow variables appear **twice** with an identical
B-part, differing only by Units (CFS vs TAF) — ~18k such dup columns. So the
unique column key is `(variable, scenario, unit)`, and the tooling **prefers
TAF** when both exist.

## Design: catalog-first, wide + MultiIndex

Two layers so we never have to load 566 MB to find something:

1. **Column catalog** — parsed from just the 7 preamble rows (instant), one row
   per data column: `col_pos, b_part, variable, scenario, unit, unit_raw,
   c_part`. This is the discovery/subset index.
2. **Values** — loaded on demand, ideally *only the columns you ask for* (via
   `usecols`), into a wide DataFrame: `DatetimeIndex` (`date`) × `MultiIndex`
   columns `(variable, scenario, unit)`, `float32`.

Subsets are selected by **explicit variable lists** (each subset is <200 vars) —
there is no prefix-based auto-classification. A tidy/long
`[date, variable, scenario, unit, value]` view is available per subset via
`to_long()` (the whole file long would be ~55M+ rows, so it's subset-only).

Alternatives considered and rejected: a fully long store (row explosion, >2 GB)
and xarray (the `variable × scenario × unit` space is ragged → dense NaN cube).

## Modules (`scripts/`)

- **`trend_report_parser.py`** — pure parsing, no DB/domain logic:
  `build_catalog(path)`, `load_values(path, col_positions=…, dtype="float32")`,
  `parse_b_part`, `normalize_unit` (`CFS.1`→`CFS`, `UMHOS/CM`→`UMHOS_CM`),
  `resolve_columns` (filter + prefer-TAF collapse), `variables_from_file`.
- **`trend_report.py`** — `TrendReport` accessor facade + CLI:
  `.load()`, `.variables`/`.scenarios`/`.units`, `.find(pattern)`,
  `.select(variables=…, scenarios=…, prefer_unit="TAF")`, `.series(...)`,
  `.to_long(...)`. Excludes `s0002` by default (see Source data).
- **`extract_wyt.py`** — Sacramento Valley water-year-type extractor. Pulls
  `WYT_SAC_`, samples **May** of each year, writes `(scenario, water_year, wyt)`
  upserts to `scenario_water_year_type`. Output: `output/wyt_sac.sql`.
- **`extract_reservoir_storage.py`** — see "Reservoir storage extract" below.

## Usage

```python
from trend_report import TrendReport

tr = TrendReport.load()                       # catalog only (default path), instant
tr.scenarios                                  # ['s0011', 's0020, ...]  (s0002 excluded)
tr.find("SHSTA")                              # search variable names

reservoirs = tr.select(                       # reads only these columns
    variables=my_reservoir_vars,              # explicit <200-var list
    scenarios=["s0011"],                      # TAF preferred automatically
)
ts   = tr.series("S_SHSTA", "s0011")          # one time series
tidy = tr.to_long(variables=my_reservoir_vars)  # [date, variable, scenario, unit, value]
```

CLI (quick inspection / ad-hoc dump):

```bash
python etl/data_in_depth/scripts/trend_report.py --summary
python etl/data_in_depth/scripts/trend_report.py --list-variables --grep "^S_"
python etl/data_in_depth/scripts/trend_report.py --dump \
    --variables-file reservoirs.txt --scenarios s0002 --out reservoirs.parquet
```

Subset variable lists can be kept as plain newline-delimited text files
(`# comments` allowed) and passed via `--variables-file` / `variables_from_file`.

## Target tables

Extracts write to these (DDL in `database/scripts/sql/`, applied via
`psql "$SUPERUSER_URL" -f …`):

- **`data_in_depth_subject`** (+ `_member`) — the flexible "related entity"
  registry. `subject_kind`: `entity` (a physical node via
  `location_type`/`location_id`, same polymorphic pair as `tier_location`),
  `aggregate` (rollup, e.g. NOD/SOD, members in `_member`, value computed at ETL),
  `metric` (non-location, e.g. X2/WYT). All extracts reference
  `data_in_depth_subject_id`. Seed reservoir + NOD/SOD subjects with
  `seed_data_in_depth_subjects.sql`.
- **`data_in_depth_value`** — generic raw long store, one row per
  `(scenario, subject, source_variable, period, water_year, unit)`. **Raw
  per-year values only** — no derived columns. A measure in multiple units =
  multiple rows (`unit_id` in the grain), e.g. volume `TAF` and
  percent-of-capacity `PCT_CAP` (PCT_CAP is a per-row transform, so it's stored;
  see "Why nothing is precomputed" below).
- **`scenario_water_year_type`** — WYT series (separate; annual classification;
  the table the WYT filter joins against).

> **Why nothing is precomputed.** Exceedance percentiles, mean, CV, and box-plot
> quantiles are all **population-dependent** — they change when a WYT filter
> restricts the set of years. So they are **not stored**; the API computes them
> live from `data_in_depth_value`, always over whatever population the request
> selects. (There is intentionally no `data_in_depth_statistic` table and no
> stored `percentile` column.) PCT_CAP is the exception: it's per-row, not a
> statistic, so it's safe to store alongside volume.

## Reservoir storage extract

`extract_reservoir_storage.py` pulls `S_<code>` storage for the 8 tier
reservoirs (TRNTY, SHSTA, OROVL, FOLSM, SLUIS_CVP, SLUIS_SWP, MELON, MLRTN),
samples **April & September** of each water year (1922–2021, 115 scenarios), and
also builds two **aggregates** — **NOD** (Trinity+Shasta+Oroville+Folsom) and
**SOD** (San Luis CVP+SWP+New Melones+Millerton) — summed across members.

For every `(subject, scenario, period, water_year)` it emits two raw rows:
- **volume** (`unit=TAF`) and **percent-of-capacity** (`unit=PCT_CAP` =
  `volume / capacity × 100`).

That's it — no percentiles, no mean/CV. Those are computed live by the API (see
"Why nothing is precomputed" above), so they stay correct when the WYT filter
changes the population of years.

> **Capacities are hardcoded in the extractor** (authoritative team values) and
> **differ from `reservoir_entity.capacity_taf`** — see [`open_issues.md`](open_issues.md).
> NOD capacity = 11,391.5 TAF, SOD = 4,983.0 TAF. Aggregate percent-of-capacity
> uses the summed member capacities.

```bash
python etl/data_in_depth/scripts/extract_reservoir_storage.py --dry-run   # summary only
python etl/data_in_depth/scripts/extract_reservoir_storage.py             # write SQL
# -> output/reservoir_storage_values.sql (460k rows)
```

Generated SQL uses `INSERT … SELECT FROM (VALUES …) JOIN data_in_depth_subject
JOIN unit [JOIN statistic_type]` with `ON CONFLICT … DO UPDATE` upserts — FK ids
are resolved by `short_code` join at apply time, so the scripts are portable.
Apply with `$DATABASE_URL` (DML → audit attribution):

```bash
psql "$DATABASE_URL" -f etl/data_in_depth/output/reservoir_storage_values.sql
```

## Conventions

- Source CSV stays under `data/` (gitignored); large/regenerable, not committed.
- Generated SQL → `etl/data_in_depth/output/` (gitignored via `etl/**/output/`),
  applied with `psql -f`.
- New table DDL → descriptively-named `create_*.sql` in `database/scripts/sql/`
  (current house convention; the numbered `sql_archive/` sequence is legacy).

