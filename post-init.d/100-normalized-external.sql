-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_external CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_external AS
SELECT *
FROM odata.external;
COMMENT ON VIEW private.normalized_odata_external IS 'Normalized external table';
