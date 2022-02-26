-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_session CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_session AS
SELECT id,
    language,
    session_number AS number,
    session_name AS name,
    abbreviation,
    start_date,
    end_date,
    title,
    type,
    modified,
    legislative_period_number AS id_legislative_period 
FROM odata.session;
COMMENT ON VIEW private.normalized_odata_session IS 'Normalized session table';

DROP VIEW IF EXISTS private.normalized_odata_session_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_session_type AS
SELECT type AS id,
    language,
    type_name
FROM odata.session
WHERE type IS NOT NULL
GROUP BY type,
    language,
    type_name;
COMMENT ON VIEW private.normalized_odata_session_type IS 'Derived from session table';
