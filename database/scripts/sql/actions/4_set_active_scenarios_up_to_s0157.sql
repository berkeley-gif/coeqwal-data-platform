-- ADD NEW SCENARIOS THAT WERE LEFT OFF INITIAL LIST
-- inserts new records into scenario table for new set of scenarios.
-- Additions needed as spreadsheet has changed since last addition.
--
-- Run from the repository root:
--   psql $DATABASE_URL -f database/scripts/sql/actions/3_add_additional_scenarios.sql
--
-- Created by Eric Lehmer 6/17/2026

BEGIN;

UPDATE scenario SET is_active = FALSE;

COMMIT;

BEGIN;

UPDATE scenario SET is_active = TRUE WHERE short_code IN ('s0011','s0020','s0021','s0023','s0024','s0025','s0026','s0027','s0028',
    's0030','s0031','s0032','s0033','s0035','s0037','s0039','s0040','s0041','s0042','s0044','s0045','s0046','s0047','s0048','s0049',
    's0050','s0051','s0056','s0057','s0058','s0059','s0060','s0062','s0063','s0065','s0067','s0068','s0069','s0071','s0072','s0073',
    's0074','s0075','s0077','s0078','s0079','s0080','s0081','s0082','s0083','s0084','s0085','s0087','s0088','s0089','s0091','s0092',
    's0093','s0094','s0095','s0097','s0098','s0099','s0100','s0101','s0102','s0103','s0104','s0105','s0107','s0108','s0109','s0110',
    's0111','s0112','s0113','s0114','s0115','s0117','s0118','s0119','s0120','s0121','s0123','s0124','s0125','s0126','s0127','s0128',
    's0129','s0130','s0131','s0133','s0134','s0135','s0136','s0137','s0138','s0139','s0140','s0141','s0143','s0144','s0145','s0146',
    's0147','s0149','s0150','s0151','s0152','s0153','s0154','s0155','s0156','s0157');

COMMIT;
