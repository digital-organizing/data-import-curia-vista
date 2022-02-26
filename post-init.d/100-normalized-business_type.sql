-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_business_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business_type AS
SELECT id,
    language,
    business_type_name AS name,
    business_type_abbreviation AS abbreviation,
    modified
FROM odata.business_type;
COMMENT ON VIEW private.normalized_odata_business_type IS 'Normalized business_type table';
