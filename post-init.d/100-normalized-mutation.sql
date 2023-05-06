-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_mutation CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_mutation AS
SELECT id,
    council_code as id_council_code,
    language,
    legislative_period_number AS id_legislative_period,
    person_number AS id_person,
    replacement_date,
    replacement_person_number AS id_replacement_person_number
FROM odata.mutation;
COMMENT ON VIEW private.normalized_odata_mutation IS 'Normalized mutation table';
