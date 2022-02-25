-- Add indexes known to be used often, but not inferable as such from the OData schema

-- person
DROP INDEX IF EXISTS odata.person_idx_person_number;
CREATE INDEX person_idx_person_number ON odata.person (person_number);

DROP INDEX IF EXISTS odata.member_council_history_idx_person_number;
CREATE INDEX member_council_history_idx_person_number ON odata.member_council_history (person_number);

DROP INDEX IF EXISTS odata.member_council_idx_person_number;
CREATE INDEX member_council_idx_person_number ON odata.member_council (person_number);

DROP INDEX IF EXISTS odata.member_parl_group_idx_person_number;
CREATE INDEX member_parl_group_idx_person_number ON odata.member_parl_group (person_number);

DROP INDEX IF EXISTS odata.voting_idx_person_number;
CREATE INDEX voting_idx_person_number ON odata.voting (person_number);

-- party
DROP INDEX IF EXISTS odata.party_idx_party_number;
CREATE INDEX party_idx_party_number ON odata.party (party_number);

DROP INDEX IF EXISTS odata.member_parl_group_party_number;
CREATE INDEX member_parl_group_party_number ON odata.member_parl_group (party_number);

-- vote
DROP INDEX IF EXISTS odata.vote_idx_id;
CREATE INDEX vote_idx_id ON odata.vote (id);
DROP INDEX IF EXISTS odata.vote_idx_language;
CREATE INDEX vote_idx_language ON odata.vote (language);

DROP INDEX IF EXISTS odata.vote_idx_registration_number;
CREATE INDEX vote_idx_registration_number ON odata.vote (registration_number);

-- voting
DROP INDEX IF EXISTS odata.voting_idx_id;
CREATE INDEX voting_idx_id ON odata.voting (id);
DROP INDEX IF EXISTS odata.voting_idx_language;
CREATE INDEX voting_idx_language ON odata.voting (language);

DROP INDEX IF EXISTS odata.voting_idx_registration_number;
CREATE INDEX voting_idx_registration_number ON odata.voting (registration_number);

DROP INDEX IF EXISTS odata.voting_idx_id_legislative_period;
CREATE INDEX voting_idx_id_legislative_period ON odata.voting (id_legislative_period);
