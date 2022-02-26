-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_publication CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_publication AS
SELECT id,
    language,
    publication_type AS type,
    sort_order,
    is_old_format,
    title,
    page,
    volume,
    year,
    modified,
    business_number
FROM odata.publication;
COMMENT ON VIEW private.normalized_odata_publication IS 'Normalized publication table';

DROP VIEW IF EXISTS private.normalized_odata_publication_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_publication_type AS
SELECT publication_type AS id,
    language,
    publication_type_name AS name,
    publication_type_abbreviation AS abbreviation
FROM odata.publication
WHERE publication_type IS NOT NULL
GROUP BY publication_type,
    language,
    publication_type_name,
    publication_type_abbreviation;
COMMENT ON VIEW private.normalized_odata_publication_type IS 'Derived from publication table';
