-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_business_role CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business_role AS
SELECT id,
    language,
    role,
    role_name,
    business_number AS id_business,
    id_external,
    parl_group_number AS id_parl_group,
    canton_number AS id_canton,
    committee_number AS id_committee,
    member_council_number AS id_member_council,
    return_type,
    modified
FROM odata.business_role;

COMMENT ON VIEW private.normalized_odata_business_role IS 'Normalized business_role table';

-- Not sure if sensible to have?
--DROP VIEW IF EXISTS private.normalized_odata_business_role_name CASCADE;
--CREATE OR REPLACE VIEW private.normalized_odata_business_role_name AS
--SELECT role AS id,
--    language,
--    role_name AS name
--FROM odata.business_role
--WHERE role IS NOT NULL
--GROUP BY role,
--    language,
--    role_name;
--COMMENT ON VIEW private.normalized_odata_business_role_YYY IS 'Derived from business_role table';
