-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_member_party CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_member_party AS
SELECT language,
    party_number AS id_party,
    person_number AS id_person,
    modified
FROM odata.member_party;
COMMENT ON VIEW private.normalized_odata_member_party IS 'Normalized member_party table';
