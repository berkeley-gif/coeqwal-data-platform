# Demand-unit polygon mapping

How entity-table `du_id` values map to polygons in
`database/seed_tables/03_GIS/du_4326.gpkg`, and what to do when no polygon
exists.

Loader: [`database/scripts/data_processing/load_du_geometries.py`](../database/scripts/data_processing/load_du_geometries.py)

Resolution rules: [`database/scripts/data_processing/du_gpkg_id_resolution.py`](../database/scripts/data_processing/du_gpkg_id_resolution.py)

Coverage scorecard: [`docs/du_geometry_gap.md`](du_geometry_gap.md)

---

## Source of truth

| Layer | File / table | Role |
|---|---|---|
| Polygons | `database/seed_tables/03_GIS/du_4326.gpkg` | One dissolved `MULTIPOLYGON` per `DU_ID` (235 ids) |
| Entity rows | `du_urban_entity`, `du_agriculture_entity`, `du_refuge_entity` | Which `du_id`s exist in the DB |
| Tier catalog | `tier_location` | Which `du_id`s appear in tier map results |

The loader writes gpkg polygons into whichever entity table already contains
the matching `du_id`. It does not create entity rows.

---

## Pattern A - alias (trailing-digit / name mismatch)

Entity id and gpkg id differ by a suffix or stem. One gpkg polygon maps to
one entity row.

| Entity `du_id` | Gpkg `DU_ID` | Notes |
|---|---|---|
| `60N_PU1` | `60N_PU` | Trailing digit on entity id |
| `90_PU5` | `90_PU` | Trailing digit on entity id |
| `72_PU` | `72_PU2` | Documented id mismatch in [`database/scripts/sql/.archive/12_mi_statistics/README.md`](../database/scripts/sql/.archive/12_mi_statistics/README.md). Seed CSV has `72_PU2`. Live DB may have `72_PU`. |

---

## Pattern B - dissolve (sub-area polygons)

Entity id is the parent name. Gpkg carries numbered sub-areas that are
unioned with PostGIS `ST_Union` at load time.

| Entity `du_id` | Gpkg `DU_ID`s unioned |
|---|---|
| `60S_PA` | `60S_PA1`, `60S_PA2` |
| `61_PA` | `61_PA1`, `61_PA2`, `61_PA3` |
| `63_PR` | `63_PR1`, `63_PR2`, `63_PR3` |
| `64_PA` | `64_PA1`, `64_PA2`, `64_PA3` |
| `71_PA` | `71_PA1` through `71_PA8` (8 sub-areas) |

---

## Pattern C - genuinely absent (roadmap)

No alias or dissolve rule applies. These `du_id`s have entity rows (or tier
staging references) but no polygon in the gpkg. Verified by querying the
gpkg directly (May 2026): zero exact, prefix, or substring matches for the
bare-code agency ids below.

Closing these gaps requires sourcing new polygons outside the gpkg. That
work is tracked here, not in loader code.

### Urban - bare-code agency ids (35)

These appear as `du_id` values in tier staging / entity tables but have no
row in the gpkg under any spelling variant:

`AMADR`, `AMCYN`, `ANTOC`, `BNCIA`, `CCWD`, `CCWDI`, `CLLPT`, `CSB038`,
`CSB103`, `CSPSO`, `CSTIC`, `CWD`, `EBMUD`, `ESB324`, `ESB347`, `ESB414`,
`ESB415`, `ESB420`, `FRFLD`, `GRSVL`, `JLIND`, `MWD`, `NAPA`, `NAPA2`,
`PCWA3`, `PINES`, `PLMAS`, `SBA029`, `SBA036`, `SCVWD`, `SUISN`, `TVAFB`,
`UPANG`, `VLLJO`, `WLDWD`

**Likely sourcing path:** agency service-area GIS from the water purveyor,
or a newer dissolve from the original CalSim GIS layers. Several of these
are SWP contractor delivery points (`CSB038`, `ESB324`, etc.) that may never
have had CalSim DU polygons in the urban gpkg.

### Urban - patterned ids without gpkg match (16)

`26N_NU513`, `60N_PA`, `60S_PU`, `61_PU1`, `61_PU2`, `63_PA`, `64_PU`,
`65_PA`, `65_PU`, `70_PA`, `70_PU1`, `71_PA` (parent id distinct from
sub-areas in Pattern B), `72_PU1`, `ELDID_NU1`, `ELDID_NU2`, `ELDID_NU3`,
`GDPUD_NU`

**Likely sourcing path:** request updated dissolve from the GIS team for
the relevant WBA, or confirm whether the id is obsolete and should be
removed from `tier_location`.

### Agriculture (12) and refuge (1)

Listed in [`docs/du_geometry_gap.md`](du_geometry_gap.md). Separate PDF
sources exist under `data/raw/pdf_tables_from_CalSim_report/` but those
PDFs do not carry polygon geometry.

---

## Expected gap reduction after Patterns A and B

Before alias/dissolve rules: 59 urban entity rows lacked gpkg polygons
([`docs/du_geometry_gap.md`](du_geometry_gap.md)).

After Patterns A and B (8 entity ids resolved): expect 51 urban rows still
without polygons, assuming the live DB contains those entity ids.

Verify on Cloud9:

```bash
python database/scripts/data_processing/load_du_geometries.py --dry-run
```

---

## Regenerating this report

```bash
python database/scripts/data_processing/load_du_geometries.py --dry-run
```

Compare `missing in gpkg` counts against this doc. When new polygons land
in `du_4326.gpkg`, update or remove the corresponding Pattern C roadmap
entry.
