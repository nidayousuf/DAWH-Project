-- DimMedicine. SCD Type 2: Med.Class, Pack.Size, Presc.Class, Legal.Controlled, Cold.Chain
--              SCD Type 1: Med.Name, ICD10.Code, Disease.Cat, Dosage.Strength, Admin.Route,
--                          InternalSKU, GTIN14, Shelf.LifeMonths
CREATE TABLE IF NOT EXISTS sdi.dim_medicine (
    med_key           INTEGER,  -- Med.Key
    med_id            VARCHAR,  -- Med.ID
    internal_sku      VARCHAR,  -- InternalSKU
    gtin14            VARCHAR,  -- GTIN14
    med_name          VARCHAR,  -- Med.Name
    icd10_code        VARCHAR,  -- ICD10.Code
    disease_cat       VARCHAR,  -- Disease.Cat
    med_class         VARCHAR,  -- Med.Class          (Type 2)
    dosage_strength   VARCHAR,  -- Dosage.Strength
    admin_route       VARCHAR,  -- Admin.Route
    pack_size         VARCHAR,  -- Pack.Size          (Type 2)
    presc_class       VARCHAR,  -- Presc.Class        (Type 2)
    legal_controlled  VARCHAR,  -- Legal.Controlled   (Type 2)
    cold_chain        VARCHAR,  -- Cold.Chain         (Type 2)
    shelf_life_months INTEGER,  -- Shelf.LifeMonths
    eff_start_date    DATE,     -- Eff.StartDate
    eff_end_date      DATE,     -- Eff.EndDate
    is_current        VARCHAR   -- Is.Current
);

-- newest version of every medicine that arrived in staging
CREATE OR REPLACE TEMP TABLE src_med AS
SELECT upper(trim(medicine_id))                         AS med_id,
       trim(internal_sku)                               AS internal_sku,
       lpad(trim(gtin14), 14, '0')                      AS gtin14,
       trim(generic_name)                               AS med_name,
       upper(trim(icd10_category))                      AS icd10_code,
       trim(disease_category)                           AS disease_cat,
       trim(therapeutic_class)                          AS med_class,
       trim(dosage_form) || ' ' || trim(strength)       AS dosage_strength,
       trim(route_of_administration)                    AS admin_route,
       pack_size || ' ' || trim(base_unit_of_measure)   AS pack_size,
       trim(prescription_class)                         AS presc_class,
       upper(trim(controlled_medicine_flag))            AS legal_controlled,
       upper(trim(cold_chain_flag))                     AS cold_chain,
       CAST(shelf_life_months AS INTEGER)               AS shelf_life_months,
       CAST(effective_from_date AS DATE)                AS effective_from_date
FROM raw.master_data
QUALIFY row_number() OVER (PARTITION BY upper(trim(medicine_id))
                           ORDER BY effective_from_date DESC, load_batch_id DESC) = 1;

-- Type 1: overwrite on every version
UPDATE sdi.dim_medicine d SET internal_sku = s.internal_sku, gtin14 = s.gtin14,
       med_name = s.med_name, icd10_code = s.icd10_code, disease_cat = s.disease_cat,
       dosage_strength = s.dosage_strength, admin_route = s.admin_route,
       shelf_life_months = s.shelf_life_months
FROM src_med s WHERE d.med_id = s.med_id;

-- Type 2: a tracked attribute changed -> close the current row
CREATE OR REPLACE TEMP TABLE med_changes AS
SELECT d.med_key, s.med_id, GREATEST(s.effective_from_date, d.eff_start_date + 1) AS new_start
FROM sdi.dim_medicine d JOIN src_med s ON d.med_id = s.med_id
WHERE d.is_current = 'Y'
  AND (d.med_class, d.pack_size, d.presc_class, d.legal_controlled, d.cold_chain)
      IS DISTINCT FROM (s.med_class, s.pack_size, s.presc_class, s.legal_controlled, s.cold_chain);

UPDATE sdi.dim_medicine d SET eff_end_date = c.new_start - 1, is_current = 'N'
FROM med_changes c WHERE d.med_key = c.med_key;

-- new medicines + new versions of changed medicines
INSERT INTO sdi.dim_medicine
SELECT (SELECT COALESCE(max(med_key), 0) FROM sdi.dim_medicine) + row_number() OVER (ORDER BY s.med_id),
       s.med_id, s.internal_sku, s.gtin14, s.med_name, s.icd10_code, s.disease_cat, s.med_class,
       s.dosage_strength, s.admin_route, s.pack_size, s.presc_class, s.legal_controlled,
       s.cold_chain, s.shelf_life_months,
       COALESCE(c.new_start, s.effective_from_date), DATE '9999-12-31', 'Y'
FROM src_med s LEFT JOIN med_changes c ON c.med_id = s.med_id
WHERE NOT EXISTS (SELECT 1 FROM sdi.dim_medicine d WHERE d.med_id = s.med_id AND d.is_current = 'Y');
