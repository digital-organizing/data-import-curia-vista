-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_objective CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_objective AS
SELECT id,
    language,
    id AS id_publication,
    publication_date,
    reference_type AS id_type,
    reference_text,
    modified,
    id_business,
    id_bill,
    referendum_deadline
FROM odata.objective;
COMMENT ON VIEW private.normalized_odata_objective IS 'Normalized objective table';

DROP VIEW IF EXISTS private.normalized_odata_objective_reference_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_objective_reference_type AS
SELECT reference_type AS id,
    language,
    reference_type_name AS name
FROM odata.objective
WHERE reference_type IS NOT NULL
GROUP BY reference_type,
    language,
    reference_type_name;
COMMENT ON VIEW private.normalized_odata_objective_reference_type IS 'Derived from objective table';
