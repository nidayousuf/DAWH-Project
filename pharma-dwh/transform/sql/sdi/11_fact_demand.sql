-- FactDemand: periodic (monthly) fact, one row per hospital, medicine and month.
CREATE SEQUENCE IF NOT EXISTS sdi.seq_demand START 1;
CREATE TABLE IF NOT EXISTS sdi.etl_loaded_demand_ids (id VARCHAR);  -- natural keys already loaded (FactDemand has no ID column)
CREATE TABLE IF NOT EXISTS sdi.fact_demand (
    demand_key            BIGINT,         -- Demand.Key
    date_key              INTEGER,        -- Date.Key (role: Demand Month, 1st of month)
    med_key               INTEGER,        -- Med.Key
    hosp_key              INTEGER,        -- Hosp.Key
    qty_forecast          INTEGER,        -- Qty.Forecast
    qty_requested         INTEGER,        -- Qty.Requested
    qty_approved          INTEGER,        -- Qty.Approved
    qty_received          INTEGER,        -- Qty.Received
    qty_consumed          INTEGER,        -- Qty.Consumed
    qty_wasted            INTEGER,        -- Qty.Wasted
    qty_unfulfilled       INTEGER,        -- Qty.Unfulfilled
    qty_closing_stock     INTEGER,        -- Qty.ClosingStock (semi-additive)
    stockout_days         INTEGER,        -- Stockout.Days
    lead_time_days        INTEGER,        -- Lead.Time.Days (non-additive)
    unfulfilled_value_usd DECIMAL(18,2),  -- Unfulfilled.Value.USD
    licensed_beds         INTEGER,        -- Licensed.Beds (semi-additive)
    icu_beds              INTEGER,        -- ICU.Beds (semi-additive)
    occupancy_pct         DECIMAL(5,2),   -- Occupancy.Pct (non-additive)
    catchment_pop         BIGINT,         -- Catchment.Pop (semi-additive)
    est_disease_cases     INTEGER,        -- Est.Disease.Cases
    disease_admissions    INTEGER,        -- Disease.Admissions
    outpatient_encounters INTEGER         -- Outpatient.Encounters
);

CREATE OR REPLACE TEMP TABLE new_demand AS
SELECT d.* FROM raw.hospital_demand d
WHERE d.load_batch_id > COALESCE((SELECT max(last_batch_id) FROM sdi.etl_watermark
                                  WHERE object_name = 'fact_demand'), 0)
  AND NOT EXISTS (SELECT 1 FROM sdi.etl_loaded_demand_ids x WHERE x.id = d.hospital_demand_record_id);

-- wholesale price valid in that month (from 01_master_data) for Unfulfilled.Value.USD
CREATE OR REPLACE TEMP TABLE price AS
SELECT upper(trim(medicine_id)) AS med_id, CAST(effective_from_date AS DATE) AS valid_from,
       wholesale_price_usd
FROM raw.master_data;

INSERT INTO sdi.fact_demand
SELECT nextval('sdi.seq_demand'),
       CAST(strftime(n.demand_month, '%Y%m%d') AS INTEGER),
       COALESCE(m.med_key, m1.med_key, -1),
       COALESCE(h.hosp_key, -1),
       n.forecast_demand_packs, n.requested_quantity_packs, n.approved_quantity_packs,
       n.received_quantity_packs, n.consumed_quantity_packs, n.wastage_quantity_packs,
       n.unfulfilled_demand_packs, n.closing_stock_packs, n.stockout_days, n.average_lead_time_days,
       round(n.unfulfilled_demand_packs *
             (SELECT p.wholesale_price_usd FROM price p
              WHERE p.med_id = upper(trim(n.medicine_id)) AND p.valid_from <= n.demand_month
              ORDER BY p.valid_from DESC LIMIT 1), 2),
       n.licensed_beds, n.icu_beds, n.average_occupancy_rate_pct, n.catchment_population,
       n.estimated_disease_cases, n.disease_related_admissions, n.outpatient_encounters
FROM new_demand n
LEFT JOIN sdi.dim_medicine m ON m.med_id = upper(trim(n.medicine_id))
      AND CAST(n.demand_month AS DATE) BETWEEN m.eff_start_date AND m.eff_end_date
LEFT JOIN (SELECT * FROM sdi.dim_medicine
           QUALIFY row_number() OVER (PARTITION BY med_id ORDER BY eff_start_date) = 1) m1
       ON m1.med_id = upper(trim(n.medicine_id))
LEFT JOIN sdi.dim_hospital h ON h.hosp_id = upper(trim(n.hospital_id))
ORDER BY n.demand_month;

INSERT INTO sdi.etl_loaded_demand_ids SELECT hospital_demand_record_id FROM new_demand;
INSERT INTO sdi.etl_watermark SELECT 'fact_demand', max(load_batch_id), now() FROM raw.hospital_demand;
