-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_bill_link CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill_link AS
SELECT id,
    language,
    id_bill,
    link_text,
    link_url,
    modified,
    link_type_id as id_link_type,
    start_date
FROM odata.bill_link;
COMMENT ON VIEW private.normalized_odata_bill is 'Normalized bill_link table';

DROP VIEW IF EXISTS private.normalized_odata_bill_link_type CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill_link_type AS
SELECT link_type_id as id,
    language,
    link_type as type
FROM odata.bill_link
WHERE link_type_id IS NOT NULL
GROUP BY link_type_id,
    language,
    link_type;
COMMENT ON VIEW private.normalized_odata_bill_link_type is 'Derived from bill_link table';
