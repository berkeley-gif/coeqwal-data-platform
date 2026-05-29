-- ADD UNIMPAIRED FLOWS ENTITY TYPE (FIXED)
-- Add new entity type with proper created_by field

INSERT INTO network_entity_type (short_code, label, description, created_by, updated_by)
VALUES (
    'unimpaired_flows',
    'Unimpaired Flows', 
    'Stream unimpaired flow',
    coeqwal_current_operator(),
    coeqwal_current_operator()
)
ON CONFLICT (short_code) DO UPDATE SET
    label = EXCLUDED.label,
    description = EXCLUDED.description,
    updated_by = coeqwal_current_operator(),
    updated_at = NOW();

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
    
    RAISE NOTICE '';
    RAISE NOTICE '✅ UNIMPAIRED FLOWS ENTITY TYPE ADDED SUCCESSFULLY';
    RAISE NOTICE '   - ID: %', new_entity_id;
    RAISE NOTICE '   - Short Code: unimpaired_flows';
    RAISE NOTICE '   - Label: Unimpaired Flows';
    RAISE NOTICE '   - Description: Stream unimpaired flow';
    RAISE NOTICE '';
    RAISE NOTICE '🎯 Use entity_type_id = % for UNIMP_ variables in network.csv', new_entity_id;
END $$;
