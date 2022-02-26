-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_rapporteur CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_rapporteur AS
SELECT id,
    language,
    business_number AS id_business,
    id_bill,
    committee_number AS id_committee,
    member_council_number as id_member_council,
    modified
FROM odata.rapporteur;
COMMENT ON VIEW private.normalized_odata_rapporteur IS 'Normalized rapporteur table';
