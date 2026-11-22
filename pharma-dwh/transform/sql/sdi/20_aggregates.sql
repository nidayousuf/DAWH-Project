-- Query-optimisation layer: summary tables rebuilt after every refresh.

-- A1. Monthly sales by medicine x pharmacy x channel (channel split, margin, KPI cards)
CREATE OR REPLACE TABLE sdi.agg_sales_month AS
SELECT d.month_start_date, f.med_key, f.pharm_key, f.location_key, f.hosp_key, f.sales_channel,
       CASE WHEN f.sales_channel = 'Ecommerce' THEN 'E-commerce'
            WHEN p.pharm_type = 'Hospital Pharmacy' THEN 'Hospital' ELSE 'Retail' END AS channel_group,
       sum(f.qty_sold) AS qty_sold, sum(f.qty_returned) AS qty_returned,
       sum(f.gross_sales_usd) AS gross_sales_usd, sum(f.discount_usd) AS discount_usd,
       sum(f.net_sales_usd) AS net_sales_usd, sum(f.cogs_usd) AS cogs_usd,
       sum(f.gross_margin_usd) AS gross_margin_usd, count(*) AS txn_count
FROM sdi.fact_sales f
JOIN sdi.dim_date d ON d.date_key = f.date_key
JOIN sdi.dim_pharmacy p ON p.pharm_key = f.pharm_key
GROUP BY ALL ORDER BY 1;

-- A2. Drill-across: hospital demand vs hospital-pharmacy sales, per month x medicine x hospital
CREATE OR REPLACE TABLE sdi.agg_demand_vs_sales AS
WITH dem AS (
    SELECT f.date_key AS month_key, m.med_id, f.hosp_key,
           sum(qty_forecast) qty_forecast, sum(qty_requested) qty_requested,
           sum(qty_received) qty_received, sum(qty_consumed) qty_consumed,
           sum(qty_unfulfilled) qty_unfulfilled, sum(unfulfilled_value_usd) unfulfilled_value_usd,
           sum(stockout_days) stockout_days
    FROM sdi.fact_demand f JOIN sdi.dim_medicine m USING (med_key) GROUP BY ALL),
sal AS (
    SELECT CAST(strftime(d.month_start_date, '%Y%m%d') AS INTEGER) AS month_key, m.med_id, f.hosp_key,
           sum(f.qty_sold) AS hosp_pharmacy_qty_sold, sum(f.net_sales_usd) AS hosp_pharmacy_net_sales_usd
    FROM sdi.fact_sales f JOIN sdi.dim_date d ON d.date_key = f.date_key
    JOIN sdi.dim_medicine m USING (med_key)
    WHERE f.hosp_key > 0 GROUP BY ALL)
SELECT dem.*, COALESCE(sal.hosp_pharmacy_qty_sold, 0) AS hosp_pharmacy_qty_sold,
       COALESCE(sal.hosp_pharmacy_net_sales_usd, 0) AS hosp_pharmacy_net_sales_usd
FROM dem LEFT JOIN sal USING (month_key, med_id, hosp_key)
ORDER BY 1;

-- A3. Medicine velocity per quarter (Top-10 fast vs slow movers)
CREATE OR REPLACE TABLE sdi.agg_medicine_velocity AS
WITH q AS (
    SELECT d.year || '-' || d.quarter AS year_quarter, m.med_id, sum(f.qty_sold) AS qty_sold,
           sum(f.net_sales_usd) AS net_sales_usd
    FROM sdi.fact_sales f JOIN sdi.dim_date d ON d.date_key = f.date_key
    JOIN sdi.dim_medicine m USING (med_key) GROUP BY ALL)
SELECT q.*, cur.med_name, cur.med_class, cur.disease_cat, cur.cold_chain,
       rank() OVER (PARTITION BY year_quarter ORDER BY qty_sold DESC) AS rank_fast,
       rank() OVER (PARTITION BY year_quarter ORDER BY qty_sold ASC)  AS rank_slow,
       CASE WHEN percent_rank() OVER (PARTITION BY year_quarter ORDER BY qty_sold) >= 0.8 THEN 'Fast-moving'
            WHEN percent_rank() OVER (PARTITION BY year_quarter ORDER BY qty_sold) <= 0.2 THEN 'Slow-moving'
            ELSE 'Medium' END AS velocity_class
FROM q JOIN sdi.dim_medicine cur ON cur.med_id = q.med_id AND cur.is_current = 'Y';
