-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_person_address CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_address AS
SELECT id,
    language,
    modified,
    person_number AS id_person,
    address_type AS type,
    address_type_name AS name,
    is_public,
    address_line1,
    address_line2,
    address_line3,
    city,
    comments,
    canton_number AS id_canton,
    postcode
FROM odata.person_address;
COMMENT ON VIEW private.normalized_odata_person_address IS 'Normalized person_address table';

DROP VIEW IF EXISTS private.normalized_odata_person_address_address_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_address_address_type AS
SELECT address_type AS id,
    language,
    address_type_name
FROM odata.person_address
WHERE address_type IS NOT NULL
GROUP BY address_type,
    language,
    address_type_name;
COMMENT ON VIEW private.normalized_odata_person_address_address_type IS 'Derived from person_address table';
