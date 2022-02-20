-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to canton_number
DROP VIEW IF EXISTS private.normalized_odata_canton CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_canton AS
SELECT id,
    language,
    canton_name AS name,
    canton_abbreviation AS abbreviation
FROM odata.canton;
COMMENT ON VIEW private.normalized_odata_canton is 'Normalized canton table';
