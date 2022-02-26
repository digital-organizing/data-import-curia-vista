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
