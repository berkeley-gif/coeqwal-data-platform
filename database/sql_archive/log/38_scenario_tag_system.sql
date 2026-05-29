-- Migration 38: Create scenario_tag and scenario_tag_link tables
-- Run as: psql $SUPERUSER_URL -f database/scripts/sql/migrations/38_scenario_tag_system.sql
--
-- Fine-grained classification tags for scenarios, distinct from the 6 broad
-- research themes in the theme table.

BEGIN;

CREATE TABLE scenario_tag (
    id              SERIAL PRIMARY KEY,
    short_code      VARCHAR NOT NULL UNIQUE,
    label           VARCHAR NOT NULL,
    description     TEXT,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by      INTEGER NOT NULL REFERENCES developer(id)
                    ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by      INTEGER NOT NULL REFERENCES developer(id)
                    ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TRIGGER audit_fields_scenario_tag
    BEFORE INSERT OR UPDATE ON scenario_tag
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE scenario_tag IS
    'Fine-grained classification tags for scenarios (e.g., baseline, groundwater, '
    'flows). Distinct from the broad research themes in the theme table.';

ALTER TABLE scenario_tag DISABLE TRIGGER USER;

INSERT INTO scenario_tag (short_code, label, description, created_by, updated_by) VALUES
    ('baseline',       'Baseline',           'Reference baseline scenarios',                            2, 2),
    ('groundwater',    'Groundwater',        'Groundwater pumping and sustainability scenarios',        2, 2),
    ('agriculture',    'Agriculture',        'Agricultural land use and irrigation demand scenarios',   2, 2),
    ('flows',          'Flows',              'Instream flow and minimum flow requirement scenarios',    2, 2),
    ('drinking_water', 'Drinking Water',     'Community water system delivery prioritization scenarios', 2, 2),
    ('infrastructure', 'Infrastructure',     'Water infrastructure modification scenarios',              2, 2),
    ('delta',          'Delta',              'Sacramento-San Joaquin Delta regulation scenarios',        2, 2),
    ('reservoir',      'Reservoir',          'Reservoir storage and carryover scenarios',                2, 2),
    ('salmon',         'Salmon',             'Salmon habitat and survival flow scenarios',               2, 2),
    ('environment',    'Environment',        'Environmental and ecological flow scenarios',              2, 2);

ALTER TABLE scenario_tag ENABLE TRIGGER USER;

CREATE TABLE scenario_tag_link (
    scenario_id     INTEGER NOT NULL REFERENCES scenario(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
    tag_id          INTEGER NOT NULL REFERENCES scenario_tag(id)
                    ON DELETE CASCADE ON UPDATE CASCADE,
    PRIMARY KEY (scenario_id, tag_id),
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by      INTEGER NOT NULL REFERENCES developer(id)
                    ON DELETE RESTRICT ON UPDATE CASCADE,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_by      INTEGER NOT NULL REFERENCES developer(id)
                    ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE INDEX idx_scenario_tag_link_reverse ON scenario_tag_link(tag_id, scenario_id);

CREATE TRIGGER audit_fields_scenario_tag_link
    BEFORE INSERT OR UPDATE ON scenario_tag_link
    FOR EACH ROW EXECUTE FUNCTION set_audit_fields();

COMMENT ON TABLE scenario_tag_link IS
    'Maps scenarios to fine-grained classification tags. '
    'A scenario can have multiple tags.';

ALTER TABLE scenario_tag_link DISABLE TRIGGER USER;

INSERT INTO scenario_tag_link (scenario_id, tag_id, created_by, updated_by)
SELECT s.id, t.id, 2, 2
FROM scenario s
CROSS JOIN scenario_tag t
WHERE (s.short_code, t.short_code) IN (
    ('s0011', 'baseline'),
    ('s0020', 'baseline'),
    ('s0021', 'baseline'),
    ('s0022', 'baseline'),
    ('s0023', 'baseline'),
    ('s0024', 'baseline'),
    ('s0025', 'groundwater'),
    ('s0026', 'groundwater'), ('s0026', 'agriculture'),
    ('s0027', 'groundwater'),
    ('s0028', 'groundwater'), ('s0028', 'agriculture'),
    ('s0029', 'flows'),
    ('s0030', 'flows'),
    ('s0031', 'flows'),
    ('s0032', 'flows'),
    ('s0033', 'flows'),
    ('s0035', 'drinking_water'),
    ('s0036', 'drinking_water'),
    ('s0037', 'drinking_water'),
    ('s0039', 'flows'), ('s0039', 'delta'),
    ('s0040', 'flows'), ('s0040', 'delta'),
    ('s0041', 'flows'), ('s0041', 'delta'),
    ('s0042', 'flows'), ('s0042', 'delta'),
    ('s0044', 'reservoir'), ('s0044', 'salmon'), ('s0044', 'agriculture'),
    ('s0045', 'delta'), ('s0045', 'flows'), ('s0045', 'environment'),
    ('s0046', 'flows'),
    ('s0065', 'infrastructure'), ('s0065', 'delta'), ('s0065', 'flows')
);

ALTER TABLE scenario_tag_link ENABLE TRIGGER USER;

GRANT SELECT, INSERT, UPDATE, DELETE ON scenario_tag TO jfantauzza;
GRANT SELECT, INSERT, UPDATE, DELETE ON scenario_tag_link TO jfantauzza;
GRANT USAGE, SELECT ON SEQUENCE scenario_tag_id_seq TO jfantauzza;

COMMIT;


SELECT 'tags' AS check, id, short_code, label
FROM scenario_tag ORDER BY id;

SELECT 'tag_counts' AS check, t.label, count(*) AS scenarios
FROM scenario_tag_link stl
JOIN scenario_tag t ON t.id = stl.tag_id
GROUP BY t.label ORDER BY t.label;

SELECT 'scenarios_with_tags' AS check, s.short_code,
       string_agg(t.label, ', ' ORDER BY t.label) AS tags
FROM scenario_tag_link stl
JOIN scenario s ON s.id = stl.scenario_id
JOIN scenario_tag t ON t.id = stl.tag_id
GROUP BY s.short_code
ORDER BY s.short_code;

\echo
\echo '38 SCENARIO TAG SYSTEM COMPLETE'
\echo '================================'
