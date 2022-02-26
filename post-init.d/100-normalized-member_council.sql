-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_member_council CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_member_council AS
SELECT id,
    language,
    id_predecessor,
    person_number as id_persion,
    active,
    canton as id_canton,
    council as id_council,
    parl_group_number as id_parl_group,
    party as id_party,
    mandates,
    additional_mandate,
    additional_activity,
    date_joining,
    date_leaving,
    date_election,
    date_oath,
    date_resignation,
    modified
FROM odata.member_council;
COMMENT ON VIEW private.normalized_odata_bill is 'Normalized member_council table; date_* columns refer to last legislature period; Query member_council_history via person_number for details';
