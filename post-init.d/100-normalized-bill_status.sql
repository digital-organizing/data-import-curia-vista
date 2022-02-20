-- Normalized (partially) Curia Vista table(s)
DROP VIEW IF EXISTS private.normalized_odata_bill_status CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill_status AS
SELECT id,
    language,
    id_bill,
    status,
    status_text,
    council,
    category AS id_bill_status_category,
    modified
FROM odata.bill_status;
COMMENT ON VIEW private.normalized_odata_bill is 'Normalized bill_status table';

DROP VIEW IF EXISTS private.normalized_odata_bill_status_category CASCADE;
CREATE OR REPLACE VIEW private.normalized_odata_bill_status_category AS
SELECT category as id,
    language,
    category_name as name
FROM odata.bill_status
WHERE category IS NOT NULL
GROUP BY category,
    language,
    category_name;
COMMENT ON VIEW private.normalized_odata_bill_status_category is 'Derived from bill_status table';
