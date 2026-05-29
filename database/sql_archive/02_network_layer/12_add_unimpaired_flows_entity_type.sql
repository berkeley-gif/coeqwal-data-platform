-- ADD UNIMPAIRED FLOWS ENTITY TYPE
-- Add new entity type for UNIMP_ variables from WRESL model runs

INSERT INTO network_entity_type (short_code, label, description, created_by, updated_by)
VALUES (
    'unimpaired_flows',
    'Unimpaired Flows', 
    'Stream unimpaired flow',
    coeqwal_current_operator(),
    coeqwal_current_operator()
)
ON CONFLICT (short_code) DO NOTHING;

\echo ''
\echo '📊 COMPLETE NETWORK_ENTITY_TYPE TABLE:'
\echo '====================================='
SELECT id, short_code, label, description 
FROM network_entity_type 
ORDER BY id;

DO $$
DECLARE
    new_entity_id INTEGER;
BEGIN
    SELECT id INTO new_entity_id 
    FROM network_entity_type 
    WHERE short_code = 'unimpaired_flows';
    
    RAISE NOTICE '✅ UNIMPAIRED FLOWS ENTITY TYPE ADDED';
    RAISE NOTICE '   - ID: %', new_entity_id;
    RAISE NOTICE '   - Short Code: unimpaired_flows';
    RAISE NOTICE '   - Label: Unimpaired Flows';
    RAISE NOTICE '';
    RAISE NOTICE '🎯 This entity type (ID: %) will be used for UNIMP_ variables', new_entity_id;
    RAISE NOTICE '   in the network registry (entity_type_id = %))', new_entity_id;
END $$;
