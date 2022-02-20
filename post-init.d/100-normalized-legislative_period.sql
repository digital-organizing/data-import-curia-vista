-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to legislative_period_number
DROP VIEW IF EXISTS private.normalized_odata_legislative_period CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_legislative_period AS
SELECT id,
    language,
    legislative_period_name AS name,
    legislative_period_abbreviation AS abbreviation,
    start_date,
    end_date
FROM odata.legislative_period;
COMMENT ON VIEW private.normalized_odata_legislative_period is 'Normalized legislative_period table';
