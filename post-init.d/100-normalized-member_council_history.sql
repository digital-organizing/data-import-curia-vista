-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_member_council_history CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_member_council_history AS
SELECT id,
    language,
    person_number AS id_person,
    date_joining,
    date_leaving,
    date_election,
    date_oath,
    date_resignation,
    modified
FROM odata.member_council_history;
COMMENT ON VIEW private.normalized_odata_bill is 'Normalized member_council_history table; Query member_council via person_number';
