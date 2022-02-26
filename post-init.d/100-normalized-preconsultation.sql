-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_preconsultation CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_preconsultation AS
SELECT id,
    language,
    id_bill,
    business_number AS id_business,
    committee_number AS id_committee,
    preconsultation_date,
    treatment_category,
    modified
FROM odata.preconsultation;
COMMENT ON VIEW private.normalized_odata_preconsultation IS 'Normalized preconsultation table';
