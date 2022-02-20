-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_bill CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill AS
SELECT id,
    language,
    id_business,
    title,
    bill_number,
    bill_type,
    modified,
    submission_date
FROM odata.bill;
COMMENT ON VIEW private.normalized_odata_bill is 'Normalized bill table';

DROP VIEW IF EXISTS private.normalized_odata_bill_link CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill_link AS
SELECT id,
    language,
    id_bill,
    link_text,
    link_url,
    modified,
    link_type_id,
    start_date
FROM odata.bill_link;
COMMENT ON VIEW private.normalized_odata_bill_link is 'Normalized bill_link table';

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
