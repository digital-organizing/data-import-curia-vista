-- Normalized (partially) Curia Vista table(s)
-- TODO: tags/tags_name
DROP VIEW IF EXISTS private.normalized_odata_business CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business AS
SELECT id,
    language,
    business_short_number,
    business_type as id_business_type,
    title,
    description,
    initial_situation,
    proceedings,
    draft_text,
    submitted_text,
    reason_text,
    documentation_text,
    motion_text,
    federal_council_proposal,
    federal_council_proposal_text,
    federal_council_proposal_date,
    submitted_by,
    submission_council AS id_council_submission,
    submission_session AS id_session_submission,
    first_council1 AS id_council_first_council1_id,
    first_council2 AS id_council_first_council2_id
FROM odata.business;
COMMENT ON VIEW private.normalized_odata_business is 'Normalized business table';

DROP VIEW IF EXISTS private.normalized_odata_business_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_business_type AS
SELECT business_type as id,
    language,
    business_type_name as name
FROM odata.business
WHERE business_type IS NOT NULL
GROUP BY business_type,
    language,
    business_type_name;
COMMENT ON VIEW private.normalized_odata_business_type is 'Derived from business table';
