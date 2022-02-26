-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_subject CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_subject AS
SELECT *
FROM odata.subject;
COMMENT ON VIEW private.normalized_odata_subject IS 'Normalized subject table';
