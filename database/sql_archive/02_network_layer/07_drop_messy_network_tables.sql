-- DROP MESSY OVERLAPPING NETWORK TABLES
-- Clean slate approach - remove tables with overlapping/confusing structure

-- =============================================================================
-- DROP MESSY TABLES - CLEAN SLATE APPROACH
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '🗑️  DROPPING MESSY OVERLAPPING TABLES';
    RAISE NOTICE '   - calsim_entity_type (replaced by network type hierarchy)';
    RAISE NOTICE '   - calsim_schematic_type (obsolete)';
    RAISE NOTICE '   - network_topology (replaced by network + connectivity)';
    RAISE NOTICE '   - network_arc (replaced by clean network structure)';
    RAISE NOTICE '   - network_node (replaced by clean network structure)';
END $$;

DROP TABLE IF EXISTS calsim_entity_type CASCADE;
DROP TABLE IF EXISTS calsim_schematic_type CASCADE;
DROP TABLE IF EXISTS network_topology CASCADE;
DROP TABLE IF EXISTS network_arc CASCADE;
DROP TABLE IF EXISTS network_node CASCADE;

DROP TABLE IF EXISTS network_arc_type CASCADE;
DROP TABLE IF EXISTS network_node_type CASCADE;
DROP TABLE IF EXISTS network_arc_subtype CASCADE;
DROP TABLE IF EXISTS network_node_subtype CASCADE;

-- =============================================================================
-- VERIFY CLEANUP
-- =============================================================================

DO $$
DECLARE
    remaining_tables TEXT[];
BEGIN
    SELECT array_agg(table_name) INTO remaining_tables
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
        AND table_name IN (
            'calsim_entity_type',
            'calsim_schematic_type', 
            'network_topology',
            'network_arc',
            'network_node',
            'network_arc_type',
            'network_node_type',
            'network_arc_subtype',
            'network_node_subtype'
        );
    
    IF remaining_tables IS NULL OR array_length(remaining_tables, 1) = 0 THEN
        RAISE NOTICE '✅ ALL MESSY TABLES SUCCESSFULLY DROPPED';
        RAISE NOTICE '';
        RAISE NOTICE '🧹 CLEANUP COMPLETE - Ready for clean implementation';
        RAISE NOTICE '';
        RAISE NOTICE '📋 Remaining network tables:';
        RAISE NOTICE '   - network_entity_type (3 records - KEEP)';
        RAISE NOTICE '   - network_gis (spatial data - KEEP for now)';
        RAISE NOTICE '';
        RAISE NOTICE '🔄 Next steps:';
        RAISE NOTICE '   1. Run 06_implement_clean_network_architecture.sql';
        RAISE NOTICE '   2. Create proper seed tables from existing data';
        RAISE NOTICE '   3. Load clean network structure';
    ELSE
        RAISE NOTICE '⚠️  Some tables still exist: %', remaining_tables;
        RAISE NOTICE '   Check for foreign key dependencies';
    END IF;
    
    RAISE NOTICE '';
    RAISE NOTICE '🧹 READY FOR CLEAN IMPLEMENTATION';
    RAISE NOTICE '   Next: Run 06_implement_clean_network_architecture.sql';
END $$;
