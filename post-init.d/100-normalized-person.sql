-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to person_number
DROP VIEW IF EXISTS private.normalized_odata_person CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person AS
SELECT id,
    language,
    title AS id_person_title,
    last_name,
    gender_as_string,
    date_of_birth,
    date_of_death,
    marital_status AS id_person_martial_status,
    place_of_birth_city,
    place_of_birth_canton,
    modified,
    first_name,
    official_name,
    military_rank AS id_person_military_rank,
    number_of_children,
    native_language
FROM odata.person;
COMMENT ON VIEW private.normalized_odata_person IS 'Normalized person table';

DROP VIEW IF EXISTS private.normalized_odata_person_title CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_title AS
SELECT title AS id,
    language,
    title_text AS text
FROM odata.person
WHERE title IS NOT NULL
GROUP BY title,
    title_text,
    language;
COMMENT ON VIEW private.normalized_odata_person_title IS 'Derived from person table';

DROP VIEW IF EXISTS private.normalized_odata_person_martial_status CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_martial_status AS
SELECT marital_status AS id,
    language,
    marital_status_text AS text
FROM odata.person
WHERE marital_status IS NOT NULL
GROUP BY marital_status,
    language,
    marital_status_text;
COMMENT ON VIEW private.normalized_odata_person_martial_status IS 'Derived from person table';

DROP VIEW IF EXISTS private.normalized_odata_person_military_rank CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_military_rank AS
SELECT military_rank AS id,
    language,
    military_rank_text AS text
FROM odata.person
WHERE military_rank IS NOT NULL
GROUP BY military_rank,
    language,
    military_rank_text;
COMMENT ON VIEW private.normalized_odata_person_military_rank IS 'Derived from person table';
