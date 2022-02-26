-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_tags CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_tags AS
SELECT *
FROM odata.tags;
COMMENT ON VIEW private.normalized_odata_tags IS 'Normalized tags table';
