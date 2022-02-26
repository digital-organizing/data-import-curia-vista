-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_business_status CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business_status AS
SELECT business_status_id AS id,
    language,
    business_status_name AS name,
    business_status_date AS data,
    business_number AS id_business,
    is_motion_in_second_council,
    new_key,
    modified
FROM odata.business_status;
COMMENT ON VIEW private.normalized_odata_business_status IS 'Normalized business_status table';
