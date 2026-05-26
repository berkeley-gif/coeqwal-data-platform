-- ============================================================
-- Migration: Add new AG aggregate entities
-- ============================================================
-- Adds settlement/exchange contractor aggregates and
-- computed NOD/SOD totals to match COEQWAL_V3 composition.
-- ============================================================

\echo 'Adding new AG aggregate entity rows...'

INSERT INTO ag_aggregate_entity
    (short_code, label, project, region, delivery_variable, description, display_order)
VALUES
    ('cvp_psc_n', 'CVP Settlement Contractors North', 'CVP', 'NOD', 'DEL_CVP_PSC_N',
     'CVP Settlement Contractors - North of Delta', 6),
    ('cvp_pex_s', 'CVP Exchange Contractors South', 'CVP', 'SOD', 'DEL_CVP_PEX_S',
     'CVP Exchange Contractors - South of Delta', 7),
    ('nod_ag', 'Total NOD AG', NULL, 'NOD', 'COMPUTED',
     'Total North of Delta AG (SWP PAG + CVP PAG + CVP Settlement). Computed as sum of components.', 8),
    ('sod_ag', 'Total SOD AG', NULL, 'SOD', 'COMPUTED',
     'Total South of Delta AG (SWP PAG + CVP PAG + CVP Exchange). Computed as sum of components.', 9)
ON CONFLICT (short_code) DO NOTHING;

\echo ''
\echo '✅ New AG aggregate entities added'
\echo ''

SELECT short_code, label, project, region, delivery_variable
FROM ag_aggregate_entity
ORDER BY display_order;
