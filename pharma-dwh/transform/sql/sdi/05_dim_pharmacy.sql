-- DimPharmacy (Type 1): 60 pharmacies - 12 hospital, 28 chain, 20 independent.
CREATE TABLE IF NOT EXISTS sdi.dim_pharmacy (
    pharm_key  INTEGER,  -- Pharm.Key
    pharm_id   VARCHAR,  -- Pharm.ID
    pharm_gln  VARCHAR,  -- Pharm.GLN
    pharm_name VARCHAR,  -- Pharm.Name
    pharm_type VARCHAR   -- Pharm.Type
);
CREATE OR REPLACE TEMP TABLE src_pharm AS
SELECT upper(trim(pharmacy_id)) AS pharm_id, lpad(trim(pharmacy_gln), 13, '0') AS pharm_gln,
       trim(pharmacy_name) AS pharm_name, trim(pharmacy_type) AS pharm_type
FROM raw.pharmacy_sales
QUALIFY row_number() OVER (PARTITION BY upper(trim(pharmacy_id))
                           ORDER BY sales_date DESC, load_batch_id DESC) = 1;

UPDATE sdi.dim_pharmacy d SET pharm_gln = s.pharm_gln, pharm_name = s.pharm_name,
       pharm_type = s.pharm_type
FROM src_pharm s WHERE d.pharm_id = s.pharm_id;

INSERT INTO sdi.dim_pharmacy
SELECT (SELECT COALESCE(max(pharm_key), 0) FROM sdi.dim_pharmacy) + row_number() OVER (ORDER BY s.pharm_id), s.*
FROM src_pharm s
WHERE NOT EXISTS (SELECT 1 FROM sdi.dim_pharmacy d WHERE d.pharm_id = s.pharm_id);
