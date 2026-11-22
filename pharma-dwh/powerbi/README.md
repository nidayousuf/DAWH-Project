# Power BI - SDI business dashboards

## 1. Connect
Get Data → **PostgreSQL database** → Server `localhost:5432`, Database `pharma_dwh`, user `postgres`, password `postgres`.
Tick these tables in schema **sdi**: `dim_date`, `dim_medicine`, `dim_location`, `dim_hospital`, `dim_pharmacy`,
`fact_sales`, `fact_demand`, `agg_medicine_velocity`. Choose **Import**.
(Do not tick `fact_sales_y2023` … these are the yearly partitions; the parent table already contains them.)

## 2. Relationships (Model view)
| From (many side) | To (one side) |
|---|---|
| fact_sales[date_key] | dim_date[date_key] |
| fact_sales[med_key] | dim_medicine[med_key] |
| fact_sales[location_key] | dim_location[location_key] |
| fact_sales[hosp_key] | dim_hospital[hosp_key] |
| fact_sales[pharm_key] | dim_pharmacy[pharm_key] |
| fact_demand[date_key] | dim_date[date_key] |
| fact_demand[med_key] | dim_medicine[med_key] |
| fact_demand[hosp_key] | dim_hospital[hosp_key] |

`agg_medicine_velocity` needs no relationship (used on its own in dashboard 3).

Mark `dim_date` as date table (column `full_date`). Sort `month_name` by `month_no`.

## 3. Channel column
In `fact_sales` add a calculated column (Table tools → New column):
```
Channel Group =
SWITCH ( TRUE (),
    fact_sales[sales_channel] = "Ecommerce", "E-commerce",
    RELATED ( dim_pharmacy[pharm_type] ) = "Hospital Pharmacy", "Hospital",
    "Retail" )
```

## 4. Measures
Copy them from `measures.dax`.

## 5. Dashboards
1. **Hospital Forecast vs Actual & Unfulfilled Demand**: cards (Fill Rate %, Forecast Accuracy %, Actual/Forecast, Unfulfilled $), line chart of Actual to Forecast by month and hospital type, matrix of Unfulfilled packs by hospital × medicine class.
2. **Channel Revenue Split & Gross Margin**: stacked column of Net Sales by quarter and Channel Group, bar chart of Gross Margin % by Channel Group, map or bar of Net Sales by region/state.
3. **Top 10 Fast vs Slow-moving Medicines**: from `agg_medicine_velocity`, slicer on `year_quarter`, bar chart Top 10 by `rank_fast`, bar chart Top 10 by `rank_slow`, filter on `cold_chain`.

Put a card with the measure **Data Through** on each page; it shows the refresh working.

## 6. Refresh
After each Airflow run press **Home → Refresh**. "Data Through" changes from 31-Dec-2025 to 31-Mar-2026 (refresh 1) and 30-Jun-2026 (refresh 2).
