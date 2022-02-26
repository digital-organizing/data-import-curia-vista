-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_member_committee_history CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_member_committee_history AS
SELECT id,
    language,
    committee_number AS id_committee,
    person_number AS id_person,
    committee_function AS function,
    committee_function_name AS function_name,
    council AS id_council,
    canton AS id_canton,
    date_joining,
    date_leaving,
    modified,
    parl_group_number AS id_parl_group
FROM odata.member_committee_history;
COMMENT ON VIEW private.normalized_odata_member_committee_history IS 'Normalized member_committee_history table';
