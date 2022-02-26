-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_resolution CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_resolution AS
SELECT id,
    language,
    resolution_number AS number,
    resolution_date AS date,
    resolution_id, -- can not name id
    resolution_text AS text,
    council AS id_counil,
    category,
    committee,
    modified,
    id_bill
FROM odata.resolution;
COMMENT ON VIEW private.normalized_odata_resolution IS 'Normalized resolution table';

DROP VIEW IF EXISTS private.normalized_odata_resolution_category CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_resolution_category AS
SELECT category AS id,
    language,
    category_name
FROM odata.resolution
WHERE category IS NOT NULL
GROUP BY category,
    language,
    category_name;
COMMENT ON VIEW private.normalized_odata_resolution_category IS 'Derived from resolution table';
