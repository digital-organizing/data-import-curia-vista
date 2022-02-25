DROP VIEW IF EXISTS private.vote_helper CASCADE;
CREATE OR REPLACE VIEW private.vote_helper AS
SELECT vote.id,
    voting.language,
    COUNT(voting.decision) AS decision_count,
    voting.decision,
    vote.subject
FROM odata.vote
    INNER JOIN odata.voting ON voting.id_vote = vote.id AND voting.language = vote.language
GROUP BY vote.id, voting.language, voting.decision, vote.subject;
COMMENT ON VIEW private.vote_helper IS 'Helper for stable.vote: Count casted voting types (yes, no, abstain, etc.) per vote.';

-- TODO: decision will behave unexpected if neither yes nor no has the highest count. Has this ever happened? What would
--       the official outcome be then?
DROP VIEW IF EXISTS stable.vote CASCADE;
CREATE OR REPLACE VIEW stable.vote AS
SELECT DISTINCT ON (id, language)
    id,
    language,
    decision,
    CASE
        WHEN lower(subject) SIMILAR TO 'schlussabstimmung|vote final|votazione finale%' THEN 'final'
        WHEN lower(subject) SIMILAR TO 'gesamtabstimmung|vote sur l''ensemble|votazione sul complesso' THEN 'plenary'
        WHEN lower(subject) SIMILAR TO 'eintrett*en|entra(re|ta) in materia|entr(ée|er) en matière' THEN 'entry'
        ELSE NULL
    END AS category
FROM private.vote_helper
ORDER BY id, language, decision, decision_count DESC;
COMMENT ON VIEW stable.vote IS 'Decisions (yes, no, etc.) made during a vote; Best guess at the vote kind (either final, plenary, entry or NULL if indeterminate).';

CREATE OR REPLACE VIEW inconsistent.vote_category AS
SELECT id
FROM stable.vote
GROUP BY id
HAVING COUNT(DISTINCT category) > 1;
COMMENT ON VIEW inconsistent.vote_category is 'Votes with contradicting classification between languages. Requires either our code to be fixed or Curia Vista data to be corrected.';
