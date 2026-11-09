CREATE TABLE IF NOT EXISTS regional_tier_mean (
    scenario_short_code VARCHAR PRIMARY KEY,
    hydroclimate VARCHAR,
    nod_ag numeric(5,3),
    sod_ag numeric(5,3),
    nod_dw numeric(5,3),
    sod_dw numeric(5,3),
    nod_eflows numeric(5,3),
    sod_eflows numeric(5,3),
    nod_gw numeric(5,3),
    sod_gw numeric(5,3),
    nod_res numeric(5,3),
    sod_res numeric(5,3),
    created_by INTEGER NOT NULL DEFAULT 2,
    updated_by INTEGER NOT NULL DEFAULT 2,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT regional_tier_mean_created_by_fkey
        FOREIGN KEY (created_by)
        REFERENCES developer(id)
        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT regional_tier_mean_updated_by_fkey
        FOREIGN KEY (updated_by) 
        REFERENCES developer(id) 
        ON DELETE RESTRICT ON UPDATE CASCADE
);
