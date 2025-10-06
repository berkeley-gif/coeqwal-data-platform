-- CREATE NETWORK_ENTITY_TYPE LOOKUP TABLE
-- Foundation table for network architecture, basic arc/node/null classification
-- To run first before creating any network tables

-- Create the network_entity_type lookup table
CREATE TABLE network_entity_type (
    id SERIAL PRIMARY KEY,
    short_code TEXT NOT NULL UNIQUE,
    label TEXT NOT NULL,
    description TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_by INTEGER NOT NULL REFERENCES developer(id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by INTEGER NOT NULL REFERENCES developer(id)
);

-- Create indexes
CREATE INDEX idx_network_entity_type_short_code ON network_entity_type(short_code);
CREATE INDEX idx_network_entity_type_active ON network_entity_type(is_active, short_code);

-- Insert the three basic infrastructure types using automatic SSO-based developer ID
-- NOTE: Requires helper functions from 00_create_helper_functions.sql
-- If functions don't exist, this will fail - run helper functions script first

INSERT INTO network_entity_type (short_code, label, description, created_by, updated_by) VALUES
('arc', 'Arc', 'CalSim network infrastructure arcs, for example channels, diversions, inflows', coeqwal_current_operator(), coeqwal_current_operator()),
('node', 'Node', 'Network infrastructure nodes - junctions, reservoirs, boundaries', coeqwal_current_operator(), coeqwal_current_operator()),
('null', 'None', 'Non-infrastructure entities - administrative, conceptual', coeqwal_current_operator(), coeqwal_current_operator());

-- FALLBACK: If helper functions don't exist, comment out above and insert the developer number from the developer table:
/*
INSERT INTO network_entity_type (short_code, label, description, created_by, updated_by) VALUES
('arc', 'Arc', 'CalSim network infrastructure arcs, for example channels, diversions, inflows', 2, 2),
('node', 'Node', 'Network infrastructure nodes - junctions, reservoirs, boundaries', 2, 2),
('null', 'None', 'Non-infrastructure entities - administrative, conceptual', 2, 2);
*/

-- Add foreign key relationships for developer audit
ALTER TABLE network_entity_type ADD CONSTRAINT fk_network_entity_type_created_by 
    FOREIGN KEY (created_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;
    
ALTER TABLE network_entity_type ADD CONSTRAINT fk_network_entity_type_updated_by 
    FOREIGN KEY (updated_by) REFERENCES developer(id) ON DELETE RESTRICT ON UPDATE CASCADE;

-- Verify the data
SELECT 
    id,
    short_code,
    label,
    description,
    is_active,
    created_by,
    (SELECT display_name FROM developer WHERE id = network_entity_type.created_by) as created_by_name
FROM network_entity_type 
ORDER BY id;

COMMENT ON TABLE network_entity_type IS 'Basic infrastructure classification for network elements: arc, node, or null';
COMMENT ON COLUMN network_entity_type.short_code IS 'Infrastructure type: arc, node, or null';
COMMENT ON COLUMN network_entity_type.label IS 'Human-readable label for the infrastructure type';
COMMENT ON COLUMN network_entity_type.description IS 'Detailed description of what this infrastructure type represents';
