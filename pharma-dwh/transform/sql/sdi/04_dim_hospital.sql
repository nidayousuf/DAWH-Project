-- DimHospital (Type 1). Extra row 0 = 'Not a hospital pharmacy' for chain / independent sales.
CREATE TABLE IF NOT EXISTS sdi.dim_hospital (
    hosp_key     INTEGER,  -- Hosp.Key
    hosp_id      VARCHAR,  -- Hosp.ID
    hosp_gln     VARCHAR,  -- Hosp.GLN
    hosp_name    VARCHAR,  -- Hosp.Name
    hosp_type    VARCHAR,  -- Hosp.Type
    trauma_level VARCHAR   -- Trauma.Level
);
INSERT INTO sdi.dim_hospital
SELECT 0, 'N/A', NULL, 'Not a hospital pharmacy', 'Not a hospital', 'Not applicable'
WHERE NOT EXISTS (SELECT 1 FROM sdi.dim_hospital WHERE hosp_key = 0);

CREATE OR REPLACE TEMP TABLE src_hosp AS
SELECT upper(trim(hospital_id)) AS hosp_id, lpad(trim(hospital_gln), 13, '0') AS hosp_gln,
       trim(hospital_name) AS hosp_name, trim(hospital_type) AS hosp_type,
       CASE WHEN trauma_level IS NULL OR trim(trauma_level) IN ('', 'None')
            THEN 'Not designated' ELSE trim(trauma_level) END AS trauma_level
FROM raw.hospital_demand
QUALIFY row_number() OVER (PARTITION BY upper(trim(hospital_id))
                           ORDER BY demand_month DESC, load_batch_id DESC) = 1;

UPDATE sdi.dim_hospital d SET hosp_gln = s.hosp_gln, hosp_name = s.hosp_name,
       hosp_type = s.hosp_type, trauma_level = s.trauma_level
FROM src_hosp s WHERE d.hosp_id = s.hosp_id;

INSERT INTO sdi.dim_hospital
SELECT (SELECT max(hosp_key) FROM sdi.dim_hospital) + row_number() OVER (ORDER BY s.hosp_id), s.*
FROM src_hosp s
WHERE NOT EXISTS (SELECT 1 FROM sdi.dim_hospital d WHERE d.hosp_id = s.hosp_id);
