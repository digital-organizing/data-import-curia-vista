-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to party_number
DROP VIEW IF EXISTS private.normalized_odata_party CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_party AS
SELECT id,
    language,
    party_name AS name,
    start_date,
    end_date,
    party_abbreviation AS abbreviation
FROM odata.party;
COMMENT ON VIEW private.normalized_odata_party IS 'Normalized party table';
