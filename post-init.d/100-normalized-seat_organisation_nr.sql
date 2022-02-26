-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_seat_organisation_nr CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_seat_organisation_nr AS
SELECT id,
    language,
    seat_number,
    person_number AS id_person,
    parl_group_number AS id_parl_group
FROM odata.seat_organisation_nr;
COMMENT ON VIEW private.normalized_odata_seat_organisation_nr IS 'Normalized seat_organisation_nr table';
