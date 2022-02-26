-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_related_business CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_related_business AS
SELECT id,
    language,
    business_number AS id_business,
    related_business_number AS id_related_business_number,
    priority_code,
    modified
FROM odata.related_business;
COMMENT ON VIEW private.normalized_odata_related_business IS 'Normalized related_business table';
