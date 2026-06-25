# Database schema documentation

## Overview

This directory holds the Entity-Relationship Diagram, [`ERD.md`](ERD.md), the reference description of every table, column, foreign key, and index in the COEQWAL Scenarios Database, organized by layer. There is a backlog, [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md).

## ERD verification

The monthly audit is the tool for checking `ERD.md` against the live database. See [`../audit/README.md`](../audit/README.md) for how to run it (`python database/audit/run_monthly_audit.py`).

Section 1b of the report (the schema-vs-ERD comparison) is currently disabled. Its comparator (`verify_erd_against_audit.py`) has drifted: it hardcodes an old ERD filename and its parser expects the old tree-format ERD, while `ERD.md` is now Markdown tables, so the comparison is silently skipped. Until it is fixed, verify the ERD by hand against the audit's table inventory (section 1a) and `schema_snapshot.json`. Tracked in [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md) § 10, with the full audit roadmap in [`../audit/README.md`](../audit/README.md#improving-the-audit).

## Entity model: MI contractors vs urban demand units

**MI (M&I contractors):** The `mi_contractor` table holds 30 rows, all State Water Project (SWP) contractors (no CVP rows are loaded yet), including agencies like Metropolitan WDSC, Alameda County WD, and Santa Clara Valley WD, plus an MWD aggregate row. CalSim tracks them explicitly by contractor name using `D_*_PMI` delivery arcs and `PERDV_SWP_*` allocation fractions. These are institutional entities, water agencies that hold Table A contracts with the State Water Project.

**DU_urban (urban demand units):** The `du_urban_entity` table holds 145 urban demand units (DUs) defined by CalSim's spatial grid. Each DU is a geographic zone like `02_PU` or `26N_NU1`. Variable mappings come from the `du_urban_variable` database table. These represent where water goes spatially, not which agency holds the contract.

An SWP contractor like MWD serves multiple DUs. A single DU might receive water from multiple projects. The two modules overlap in coverage but answer different questions. MI tells you "how much did a given contractor get?", DU_urban tells you "how much water reached a given geographic zone?"

For the full table-by-table breakdown of demand units, contractors, CWS aggregates, and how they relate (with verified row counts), see [`../topic_docs/cws/water_user_categories.md`](../topic_docs/cws/water_user_categories.md).

## Updating the ERD

When adding new tables:
1. Add the SQL script to `../scripts/sql/` with appropriate numbering (see [`../scripts/sql/README.md`](../scripts/sql/README.md)).
2. Document the table in [`ERD.md`](ERD.md).
3. Update the schema implementation status in [`../README.md`](../README.md).
4. Re-run the monthly audit and compare `ERD.md` against the table inventory (section 1a) by hand, since section 1b is currently disabled (see [ERD verification](#erd-verification) above).