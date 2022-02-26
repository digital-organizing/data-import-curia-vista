-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_person_interest CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_interest AS
SELECT id,
    language,
    organization_type,
    function_in_agency,
    person_number AS id_person,
    modified,
    interest_type,
    interest_name,
    sort_order,
    paid
FROM odata.person_interest;
COMMENT ON VIEW private.normalized_odata_person_interest IS 'Normalized person_interest table';

DROP VIEW IF EXISTS private.normalized_odata_person_interest_organization_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_interest_organization_type AS
SELECT organization_type AS id,
    language,
    organization_type_text,
    organization_type_short_text
FROM odata.person_interest
WHERE organization_type IS NOT NULL
GROUP BY organization_type,
    language,
    organization_type_text,
    organization_type_short_text;
COMMENT ON VIEW private.normalized_odata_person_interest_organization_type IS 'Derived from person_interest table';

DROP VIEW IF EXISTS private.normalized_odata_person_interest_function_in_agency CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_interest_function_in_agency AS
SELECT function_in_agency AS id,
    language,
    function_in_agency_text,
    function_in_agency_short_text
FROM odata.person_interest
WHERE function_in_agency IS NOT NULL
GROUP BY function_in_agency,
    language,
    function_in_agency_text,
    function_in_agency_short_text;
COMMENT ON VIEW private.normalized_odata_person_interest_function_in_agency IS 'Derived from person_interest table';

DROP VIEW IF EXISTS private.normalized_odata_person_interest_interest_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_person_interest_interest_type AS
SELECT interest_type AS id,
    language,
    interest_type_text,
    interest_type_short_text
FROM odata.person_interest
WHERE interest_type IS NOT NULL
GROUP BY interest_type,
    language,
    interest_type_text,
    interest_type_short_text;
COMMENT ON VIEW private.normalized_odata_person_interest_interest_type IS 'Derived from person_interest table';
