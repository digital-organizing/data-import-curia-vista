-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_business_responsibility CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business_responsibility AS
SELECT id,
    language,
    business_number,
    department_number as id_department,
    is_leading as is_leading_department,
    modified,
    bill_number
FROM odata.business_responsibility;
COMMENT ON VIEW private.normalized_odata_business_responsibility IS 'Normalized business_responsibility table';
