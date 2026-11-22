-- FactSales: transaction fact, one row per sale (SalesTransactionID), all pharmacy types.
-- Incremental: only new staging batches (watermark) and only IDs not already loaded.
-- Med.Key is looked up point-in-time: the DimMedicine version valid on the sale date (SCD 2).
CREATE TABLE IF NOT EXISTS sdi.etl_watermark (object_name VARCHAR, last_batch_id BIGINT, updated_at TIMESTAMP);
CREATE SEQUENCE IF NOT EXISTS sdi.seq_sales START 1;
CREATE TABLE IF NOT EXISTS sdi.fact_sales (
    sales_key        BIGINT,         -- Sales.Key
    date_key         INTEGER,        -- Date.Key  (role: Sales Date)
    med_key          INTEGER,        -- Med.Key
    location_key     INTEGER,        -- Location.Key (pharmacy's city)
    hosp_key         INTEGER,        -- Hosp.Key (0 = not a hospital pharmacy)
    pharm_key        INTEGER,        -- Pharm.Key
    sales_txn_id     VARCHAR,        -- Sales.Txn.ID (degenerate)
    sale_status      VARCHAR,        -- Sale.Status
    return_flag      VARCHAR,        -- Return.Flag
    sales_channel    VARCHAR,        -- Sales.Channel
    qty_sold         INTEGER,        -- Qty.Sold
    qty_returned     INTEGER,        -- Qty.Returned
    sales_tax_usd    DECIMAL(18,2),  -- Sales.Tax.USD
    gross_sales_usd  DECIMAL(18,2),  -- Gross.Sales.USD
    discount_usd     DECIMAL(18,2),  -- Discount.USD
    net_sales_usd    DECIMAL(18,2),  -- Net.Sales.USD
    cogs_usd         DECIMAL(18,2),  -- COGS.USD
    gross_margin_usd DECIMAL(18,2),  -- Gross.Margin.USD
    days_to_expiry   INTEGER         -- Days.To.Expiry
);

CREATE OR REPLACE TEMP TABLE new_sales AS
SELECT s.* FROM raw.pharmacy_sales s
WHERE s.load_batch_id > COALESCE((SELECT max(last_batch_id) FROM sdi.etl_watermark
                                  WHERE object_name = 'fact_sales'), 0)
  AND NOT EXISTS (SELECT 1 FROM sdi.fact_sales f WHERE f.sales_txn_id = s.sales_transaction_id);

INSERT INTO sdi.fact_sales
SELECT nextval('sdi.seq_sales'),
       CAST(strftime(n.sales_date, '%Y%m%d') AS INTEGER),
       COALESCE(m.med_key, m1.med_key, -1),
       COALESCE(l.location_key, -1),
       CASE WHEN n.hospital_id IS NULL THEN 0 ELSE COALESCE(h.hosp_key, -1) END,
       COALESCE(p.pharm_key, -1),
       n.sales_transaction_id, trim(n.sale_status), upper(trim(n.return_flag)), trim(n.sales_channel),
       n.quantity_sold_packs, n.returned_quantity_packs, n.sales_tax_usd,
       n.gross_sales_amount_usd, n.discount_amount_usd, n.net_sales_amount_usd,
       n.cost_of_goods_sold_usd, n.gross_margin_usd, n.days_to_expiry_at_sale
FROM new_sales n
LEFT JOIN sdi.dim_medicine m ON m.med_id = upper(trim(n.medicine_id))
      AND CAST(n.sales_date AS DATE) BETWEEN m.eff_start_date AND m.eff_end_date
LEFT JOIN (SELECT * FROM sdi.dim_medicine
           QUALIFY row_number() OVER (PARTITION BY med_id ORDER BY eff_start_date) = 1) m1
       ON m1.med_id = upper(trim(n.medicine_id))
LEFT JOIN sdi.dim_location l ON l.city = trim(n.pharmacy_city)
      AND l.state = upper(trim(n.pharmacy_state)) AND l.country = upper(trim(n.country_code))
LEFT JOIN sdi.dim_hospital h ON h.hosp_id = upper(trim(n.hospital_id))
LEFT JOIN sdi.dim_pharmacy p ON p.pharm_id = upper(trim(n.pharmacy_id))
ORDER BY n.sales_date;   -- rows stored in date order -> date filters skip whole blocks

INSERT INTO sdi.etl_watermark SELECT 'fact_sales', max(load_batch_id), now() FROM raw.pharmacy_sales;
