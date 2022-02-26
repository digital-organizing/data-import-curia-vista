-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to committee_number
DROP VIEW IF EXISTS private.normalized_odata_committee CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_committee AS
SELECT id,
    language,
    main_committee_number,
    sub_committee_number,
    committee_name AS name,
    abbreviation,
    abbreviation1,
    abbreviation2,
    council AS id_council,
    modified,
    committee_type,
    display_type
FROM odata.committee;
COMMENT ON VIEW private.normalized_odata_committee IS 'Normalized committee table';

DROP VIEW IF EXISTS private.normalized_odata_committee_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_committee_type AS
SELECT committee_type AS id,
    language,
    committee_type_name AS name,
    committee_type_abbreviation as abbreviation
FROM odata.committee
WHERE committee_type IS NOT NULL
GROUP BY committee_type,
    language,
    committee_type_name,
    committee_type_abbreviation;
COMMENT ON VIEW private.normalized_odata_committee_type IS 'Derived from committee table';
