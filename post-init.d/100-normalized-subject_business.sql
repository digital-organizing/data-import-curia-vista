-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_subject_business CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_subject_business AS
SELECT id_subject,
    language,
    business_number AS id_business,
    title,
    sort_order,
    published_notes,
    modified,
    title_de,
    title_fr,
    title_it
FROM odata.subject_business;
COMMENT ON VIEW private.normalized_odata_subject_business IS 'Normalized subject_business table';
