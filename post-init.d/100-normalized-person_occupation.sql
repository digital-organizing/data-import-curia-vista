-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_person_occupation CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_occupation AS
SELECT id,
    language,
    person_number AS id_person,
    occupation,
    occupation_name,
    start_date,
    end_date,
    modified,
    employer,
    job_title
FROM odata.person_occupation;
COMMENT ON VIEW private.normalized_odata_person_occupation IS 'Normalized person_occupation table';
