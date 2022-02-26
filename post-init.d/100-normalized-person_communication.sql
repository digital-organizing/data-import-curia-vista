-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_person_communication CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_communication AS
SELECT id,
    language,
    person_number AS id_person,
    address,
    communication_type AS type,
    modified
FROM odata.person_communication;
COMMENT ON VIEW private.normalized_odata_person_communication IS 'Normalized person_communication table';

DROP VIEW IF EXISTS private.normalized_odata_person_communication__communication_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_communication__communication_type AS
SELECT communication_type AS id,
    language,
    communication_type_text
FROM odata.person_communication
WHERE communication_type IS NOT NULL
GROUP BY communication_type,
    language,
    communication_type_text;
COMMENT ON VIEW private.normalized_odata_person_communication__communication_type IS 'Derived from person_communication table';
