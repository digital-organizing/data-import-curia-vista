-- Normalized (partially) Curia Vista table(s)
-- TODO: Try harder to normalize this mess
DROP VIEW IF EXISTS private.normalized_odata_transcript CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_transcript AS
SELECT id,
    language,
    id_subject,
    vote_id AS id_vote,
    person_number AS id_person,
    type,
    text,
    meeting_council_abbreviation,
    meeting_date,
    meeting_verbalix_oid,
    id_session,
    speaker_first_name,
    speaker_last_name,
    speaker_full_name,
    speaker_function,
    council_id AS id_council,
    canton_id AS id_canton,
    parl_group_name,
    parl_group_abbreviation,
    sort_order,
    start,
    "end",
    "function",
    display_speaker,
    language_of_text,
    modified,
    start_time_with_timezone,
    end_time_with_timezone,
    vote_business_number AS id_vote_business_number
FROM odata.transcript;
COMMENT ON VIEW private.normalized_odata_transcript IS 'Normalized transcript table';
