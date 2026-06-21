# Database schema documentation

## Overview

This directory holds the Entity-Relationship Diagram, [`ERD.md`](ERD.md), the reference description of every table, column, foreign key, and index in the COEQWAL Scenarios Database, organized by layer. There is a backlog, [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md).

## ERD verification

The ERD is checked against the live database by the monthly audit. See [`../audit/README.md`](../audit/README.md) for how to run it (`python database/audit/run_monthly_audit.py`). Section 1b of the report is the schema-vs-ERD comparison.

The automated comparator (`verify_erd_against_audit.py`) has drifted. It hardcodes an old ERD filename and its parser expects the old tree-format ERD, while `ERD.md` is now Markdown tables, so the comparison is silently skipped. Until it is fixed (tracked in [`../SCHEMA_BACKLOG.md`](../SCHEMA_BACKLOG.md) § 10), confirm the ERD by reading section 1b of the audit report by hand.

## Entity model: MI contractors vs urban demand units

**MI (M&I contractors):** These are the 16 named SWP contractor agencies (MWD, Alameda County, Kern County, Santa Clara Valley, etc.) plus an MWD aggregate. CalSim tracks them explicitly by contractor name using `D_*_PMI` delivery arcs and `PERDV_SWP_*` allocation fractions. These are institutional entities, water agencies that have Table A contracts with the State Water Project.

**DU_urban (urban demand units):** These are 81 geographic demand units (DUs) defined by CalSim's spatial grid. Each DU is a geographic zone like `10_SBA1`, `26S_SJR1`, etc. Variable mappings come from the `du_urban_variable` database table. These represent where water goes spatially, not which agency holds the contract.

An SWP contractor like MWD serves multiple DUs. A single DU might receive water from multiple projects. The two modules overlap in coverage but answer different questions. MI tells you "how much did Kern County Water Agency get?", DU_urban tells you "how much water reached geographic zone 26S?"

## Updating the ERD

When adding new tables:
1. Add the SQL script to `../scripts/sql/` with appropriate numbering (see [`../scripts/sql/README.md`](../scripts/sql/README.md)).
2. Document the table in [`ERD.md`](ERD.md).
3. Update the schema implementation status in [`../README.md`](../README.md).
4. Re-run the monthly audit and review section 1b for drift.