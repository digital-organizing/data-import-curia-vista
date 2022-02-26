-- Curia Vista table vote (partially) normalized and extended with vote category
DROP VIEW IF EXISTS private.normalized_odata_vote CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_vote AS
SELECT id,
       language,
       subject,
       meaning_yes,
       meaning_no,
       vote_end,
       business_number AS id_business,
       id_session
FROM odata.vote;
COMMENT ON VIEW private.normalized_odata_vote IS 'Normalized vote bill';
