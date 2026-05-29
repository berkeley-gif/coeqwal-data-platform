-- UPDATE NETWORK_ENTITY_TYPE DESCRIPTIONS
-- Update all three type descriptions with new cleaner definitions

UPDATE network_entity_type 
SET description = 'CalSim network arcs, for example, channels, diversions, and inflows',
    updated_by = coeqwal_current_operator(),
    updated_at = NOW()
WHERE short_code = 'arc';

UPDATE network_entity_type 
SET description = 'CalSim network nodes, for example, junctions, reservoirs, stream gauges, pumping plants, and demands',
    updated_by = coeqwal_current_operator(),
    updated_at = NOW()
WHERE short_code = 'node';

UPDATE network_entity_type 
SET description = 'Non-CalSim network',
    updated_by = coeqwal_current_operator(),
    updated_at = NOW()
WHERE short_code = 'null';

SELECT id, short_code, label, description, updated_at, updated_by 
FROM network_entity_type 
ORDER BY id;
