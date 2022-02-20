-- Normalized (partially) Curia Vista table(s)
-- See https://gitlab.com/votelog/data-provider-curia-vista/-/issues/3
DROP VIEW IF EXISTS private.normalized_odata_council CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_council AS
SELECT id,
    language,
    council_name AS name,
    council_abbreviation AS abbreviation
    FROM odata.council
    UNION SELECT *
        FROM (VALUES
            (99, 'DE', 'Bundesrat', 'BR'),
            (99, 'FR', 'Conseil fédéral', 'CF'),
            (99, 'IT', 'Consiglio federale', 'CF'),
            (99, 'RM', 'Cussegl federal', 'CF'),
            (99, 'EN', 'Federal council', 'FC')
        ) AS fixup_data;
COMMENT ON VIEW private.normalized_odata_council is 'Normalized council table';
