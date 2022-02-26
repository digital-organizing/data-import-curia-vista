-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_citizenship CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_citizenship AS
SELECT id,
    language,
    person_number AS id_person,
    post_code,
    city,
    canton_abbreviation,
    modified
FROM odata.citizenship;
COMMENT ON VIEW private.normalized_odata_citizenship IS 'Normalized citizenship table';
