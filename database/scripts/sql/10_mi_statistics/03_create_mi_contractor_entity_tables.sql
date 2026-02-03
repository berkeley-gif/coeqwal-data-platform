-- CREATE MI_CONTRACTOR ENTITY TABLES
-- M&I (Municipal & Industrial) contractor entities for SWP/CVP water deliveries
-- Following the reservoir_entity/reservoir_group pattern
--
-- Tables created:
--   1. mi_contractor_group - Groups of contractors (SWP, CVP_NOD, CVP_SOD, etc.)
--   2. mi_contractor - Individual contractors (agencies)
--   3. mi_contractor_group_member - Many-to-many relationship
--   4. mi_contractor_delivery_arc - Mapping delivery variables to contractors
--
-- Prerequisites:
--   1. developer table exists (for FK)
--
-- Run with: psql -f 03_create_mi_contractor_entity_tables.sql

\echo ''
\echo '========================================='
\echo 'CREATING MI_CONTRACTOR ENTITY TABLES'
\echo '========================================='

-- ============================================
-- DROP EXISTING TABLES (for clean recreation)
-- ============================================
DROP TABLE IF EXISTS mi_contractor_delivery_arc CASCADE;
DROP TABLE IF EXISTS mi_contractor_group_member CASCADE;
DROP TABLE IF EXISTS mi_contractor CASCADE;
DROP TABLE IF EXISTS mi_contractor_group CASCADE;

-- ============================================
-- 1. MI_CONTRACTOR_GROUP (like reservoir_group)
-- ============================================
\echo ''
\echo 'Creating mi_contractor_group table...'

CREATE TABLE mi_contractor_group (
    id INTEGER PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,     -- swp, cvp_nod, cvp_sod, all_mi
    label VARCHAR(100) NOT NULL,
    description TEXT,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- ============================================
-- 2. MI_CONTRACTOR (like reservoir_entity)
-- ============================================
\echo ''
\echo 'Creating mi_contractor table...'

CREATE TABLE mi_contractor (
    id SERIAL PRIMARY KEY,
    short_code VARCHAR(50) UNIQUE NOT NULL,     -- ACWD, MWD, CVP_SOD_AG, etc.
    contractor_name VARCHAR(100) NOT NULL,

    -- Classification
    project VARCHAR(10) NOT NULL,               -- SWP, CVP
    region VARCHAR(10),                         -- NOD, SOD, null
    contractor_type VARCHAR(10) NOT NULL,       -- MI, MWD, AG, EX (Exchange), RF (Refuge), LS (Losses)

    -- Contract amounts
    contract_amount_taf NUMERIC(10,2),          -- Table A (SWP) or contract amount (CVP)

    -- Source reference
    source_contractor_id INTEGER,               -- Original ID from wresl/table files
    source_file VARCHAR(100),                   -- swp_contractor_perdel_A.wresl, nodcvpcontract.table, etc.

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1
);

-- ============================================
-- 3. MI_CONTRACTOR_GROUP_MEMBER (like reservoir_group_member)
-- ============================================
\echo ''
\echo 'Creating mi_contractor_group_member table...'

CREATE TABLE mi_contractor_group_member (
    id SERIAL PRIMARY KEY,
    mi_contractor_group_id INTEGER NOT NULL REFERENCES mi_contractor_group(id) ON DELETE CASCADE,
    mi_contractor_id INTEGER NOT NULL REFERENCES mi_contractor(id) ON DELETE CASCADE,
    display_order INTEGER DEFAULT 0,

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_mi_contractor_group_member UNIQUE(mi_contractor_group_id, mi_contractor_id)
);

-- ============================================
-- 4. MI_CONTRACTOR_DELIVERY_ARC
-- ============================================
\echo ''
\echo 'Creating mi_contractor_delivery_arc table...'

CREATE TABLE mi_contractor_delivery_arc (
    id SERIAL PRIMARY KEY,
    mi_contractor_id INTEGER NOT NULL REFERENCES mi_contractor(id) ON DELETE CASCADE,
    delivery_arc VARCHAR(50) NOT NULL,          -- D_SBA029_ACWD_PMI, D7_AG, etc.
    arc_type VARCHAR(20),                       -- PMI, PAG, PIN, PLS, etc.

    -- Audit fields
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL DEFAULT 1,

    -- Constraints
    CONSTRAINT uq_delivery_arc UNIQUE(delivery_arc)
);

-- ============================================
-- INDEXES
-- ============================================
\echo ''
\echo 'Creating indexes...'

-- mi_contractor indexes
CREATE INDEX idx_mi_contractor_short_code ON mi_contractor(short_code);
CREATE INDEX idx_mi_contractor_project ON mi_contractor(project);
CREATE INDEX idx_mi_contractor_type ON mi_contractor(contractor_type);
CREATE INDEX idx_mi_contractor_region ON mi_contractor(region);

-- mi_contractor_group_member indexes
CREATE INDEX idx_mi_contractor_group_member_group ON mi_contractor_group_member(mi_contractor_group_id);
CREATE INDEX idx_mi_contractor_group_member_contractor ON mi_contractor_group_member(mi_contractor_id);

-- mi_contractor_delivery_arc indexes
CREATE INDEX idx_mi_contractor_delivery_arc_contractor ON mi_contractor_delivery_arc(mi_contractor_id);
CREATE INDEX idx_mi_contractor_delivery_arc_arc ON mi_contractor_delivery_arc(delivery_arc);

-- ============================================
-- COMMENTS
-- ============================================
COMMENT ON TABLE mi_contractor_group IS 'Groups of M&I contractors (SWP, CVP_NOD, CVP_SOD, ALL_MI). Analogous to reservoir_group.';
COMMENT ON TABLE mi_contractor IS 'M&I contractor entities (water agencies). Analogous to reservoir_entity.';
COMMENT ON TABLE mi_contractor_group_member IS 'Many-to-many relationship between contractors and groups. Analogous to reservoir_group_member.';
COMMENT ON TABLE mi_contractor_delivery_arc IS 'Mapping of CalSim delivery arc variables to contractors.';

COMMENT ON COLUMN mi_contractor.project IS 'Water project: SWP (State Water Project) or CVP (Central Valley Project)';
COMMENT ON COLUMN mi_contractor.region IS 'Region: NOD (North of Delta), SOD (South of Delta), or NULL';
COMMENT ON COLUMN mi_contractor.contractor_type IS 'Type: MI (Municipal & Industrial), MWD (Metropolitan Water District), AG (Agricultural), EX (Exchange), RF (Refuge), LS (Losses)';
COMMENT ON COLUMN mi_contractor.contract_amount_taf IS 'Contract amount in TAF (Table A for SWP)';
COMMENT ON COLUMN mi_contractor_delivery_arc.delivery_arc IS 'CalSim delivery arc variable name (e.g., D_SBA029_ACWD_PMI)';
COMMENT ON COLUMN mi_contractor_delivery_arc.arc_type IS 'Delivery arc type: PMI (Project M&I), PAG (Project Ag), PIN (Project Industrial), etc.';

-- ============================================
-- VERIFICATION
-- ============================================
\echo ''
\echo '✅ MI_CONTRACTOR entity tables created successfully'
\echo ''
\echo 'Tables created:'
SELECT table_name,
       (SELECT COUNT(*) FROM information_schema.columns c WHERE c.table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('mi_contractor_group', 'mi_contractor', 'mi_contractor_group_member', 'mi_contractor_delivery_arc')
ORDER BY table_name;

\echo ''
\echo 'Foreign keys:'
SELECT
    tc.table_name,
    tc.constraint_name,
    kcu.column_name,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
FROM information_schema.table_constraints AS tc
JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
WHERE tc.constraint_type = 'FOREIGN KEY'
  AND tc.table_name IN ('mi_contractor_group_member', 'mi_contractor_delivery_arc');
