-- Normalized (partially) Curia Vista table(s)
-- TODO: Test for id always being equal to meeting_number
DROP VIEW IF EXISTS private.normalized_odata_meeting CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_meeting AS
SELECT id,
    language,
    id_session,
    council AS id_council,
    "date",
    "begin",
    modified,
    publication_status,
    meeting_order_text,
    sort_order,
    location
FROM odata.meeting;
COMMENT ON VIEW private.normalized_odata_meeting IS 'Normalized meeting table';
