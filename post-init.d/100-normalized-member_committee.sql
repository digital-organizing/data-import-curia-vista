-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_member_committee CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_member_committee AS
SELECT id,
    language,
    committee_number AS id_committee,
    person_number AS id_person,
    committee_function AS function,
    committee_function_name AS function_name,
    council AS id_council,
    canton AS id_canton,
    modified,
    parl_group_number AS id_parl_group
FROM odata.member_committee;
COMMENT ON VIEW private.normalized_odata_member_committee IS 'Normalized member_committee table';
