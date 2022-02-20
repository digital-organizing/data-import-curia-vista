-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_voting CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_voting AS
SELECT id,
    language,
    id_vote,
    person_number AS id_person,
    decision AS id_voting_decision
FROM odata.voting;
COMMENT ON VIEW private.normalized_odata_voting IS 'Normalized voting table';

DROP VIEW IF EXISTS private.normalized_odata_voting_decision CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_voting_decision AS
SELECT decision AS id,
    language,
    decision_text AS text
FROM odata.voting
WHERE decision IS NOT NULL
GROUP BY decision,
    language,
    decision_text;
COMMENT ON VIEW private.normalized_odata_voting_decision IS 'Derived from voting table';
