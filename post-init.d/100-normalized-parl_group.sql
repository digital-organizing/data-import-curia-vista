-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_parl_group CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_parl_group AS
SELECT id,
    language,
    is_active,
    parl_group_code AS code,
    parl_group_name AS name,
    name_used_since,
    parl_group_abbreviation AS abbreviation,
    parl_group_colour AS colour
FROM odata.parl_group;
COMMENT ON VIEW private.normalized_odata_parl_group is 'Normalized parl_group table';
