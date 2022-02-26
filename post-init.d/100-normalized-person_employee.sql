-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_person_employee CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_employee AS
SELECT id,
    language,
    person_number AS id_person,
    employer,
    job_title,
    modified
FROM odata.person_employee;
COMMENT ON VIEW private.normalized_odata_person_employee IS 'Normalized person_employee table';
