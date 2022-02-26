-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to parl_group_number
DROP VIEW IF EXISTS private.normalized_odata_parl_group CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_parl_group AS
SELECT id,
    language,
    parl_group_number AS number,
    parl_group_colour AS colour,
    is_active,
    parl_group_name AS name,
    parl_group_abbreviation AS abbreviation,
    name_used_since,
    modified
FROM odata.parl_group_history;
COMMENT ON VIEW private.normalized_odata_parl_group is 'Normalized parl_group table';
