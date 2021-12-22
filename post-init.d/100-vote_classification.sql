CREATE VIEW private.vote_classification AS
SELECT id as vote_id,
       language,
       CASE
           WHEN lower(subject) SIMILAR TO 'schlussabstimmung|vote final|votazione finale%' THEN 'final'
           WHEN lower(subject) SIMILAR TO 'gesamtabstimmung|vote sur l''ensemble|votazione sul complesso' THEN 'plenary'
           WHEN lower(subject) SIMILAR TO 'eintrett*en|entra(re|ta) in materia|entr(ée|er) en matière' THEN 'entry'
           END AS category
FROM odata.vote;

CREATE OR REPLACE VIEW stable.vote AS
SELECT vote_id, category
FROM private.vote_classification;
COMMENT ON VIEW stable.vote is 'Best guess at the vote kind (either final, plenary or entry). NULL if unknown.';

CREATE OR REPLACE VIEW inconsistent.vote_classification AS
SELECT vote_id
FROM private.vote_classification
GROUP BY vote_id
HAVING COUNT(DISTINCT category) > 1;
COMMENT ON VIEW private.vote_classification is 'Votes with contradicting classification between languages. Requires either our code to be fixed or Curia Vista data to be corrected.';
