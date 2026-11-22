"""Analytical (OLAP) queries on the SDI mart. Each query has a BASE version on the star schema
and, where possible, an OPTIMISED version that reads a pre-aggregated summary table.
run_queries.py times both so the Query dashboard shows the effect of the summary tables."""

CHANNEL = """CASE WHEN f.sales_channel = 'Ecommerce' THEN 'E-commerce'
                  WHEN p.pharm_type = 'Hospital Pharmacy' THEN 'Hospital' ELSE 'Retail' END"""
LAST_Q = """(SELECT max(d.year || '-' || d.quarter) FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key))"""

QUERIES = [
    dict(name="Q01_kpi_cards", widget="KPI cards: Net Sales, GM %, Packs, Sales",
         stakeholder="CFO / CCO", olap="Roll-up to grand total",
         question="What are total net sales, gross margin %, packs sold and number of sales?",
         base="""SELECT sum(net_sales_usd), round(100*sum(gross_margin_usd)/sum(net_sales_usd),2), sum(qty_sold), count(*)
                 FROM sdi.fact_sales""",
         optimized="""SELECT sum(net_sales_usd), round(100*sum(gross_margin_usd)/sum(net_sales_usd),2), sum(qty_sold), sum(txn_count)
                      FROM sdi.agg_sales_month"""),
    dict(name="Q02_channel_split", widget="Channel revenue split & gross margin by quarter",
         stakeholder="CCO / CFO", olap="Slice & dice (channel x quarter)",
         question="Net sales and gross margin % of Retail, Hospital and E-commerce each quarter.",
         base=f"""SELECT d.year, d.quarter, {CHANNEL} AS channel_group, sum(f.net_sales_usd) net_sales,
                        round(100*sum(f.gross_margin_usd)/sum(f.net_sales_usd),2) gm_pct
                 FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_pharmacy p USING (pharm_key)
                 GROUP BY 1,2,3 ORDER BY 1,2,3""",
         optimized="""SELECT year(month_start_date), 'Q' || quarter(month_start_date), channel_group, sum(net_sales_usd),
                             round(100*sum(gross_margin_usd)/sum(net_sales_usd),2)
                      FROM sdi.agg_sales_month GROUP BY 1,2,3 ORDER BY 1,2,3"""),
    dict(name="Q03_top10_fast", widget="Top 10 fast-moving medicines (latest quarter)",
         stakeholder="CSCO", olap="Ranking (Top-N)",
         question="Which 10 medicines sold the most packs in the latest quarter?",
         base=f"""SELECT m.med_name, m.med_class, m.cold_chain, sum(f.qty_sold) packs
                  FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_medicine m USING (med_key)
                  WHERE d.year || '-' || d.quarter = {LAST_Q}
                  GROUP BY ALL ORDER BY packs DESC LIMIT 10""",
         optimized="""SELECT med_name, med_class, cold_chain, qty_sold FROM sdi.agg_medicine_velocity
                      WHERE year_quarter = (SELECT max(year_quarter) FROM sdi.agg_medicine_velocity)
                      ORDER BY rank_fast LIMIT 10"""),
    dict(name="Q04_bottom10_slow", widget="Top 10 slow-moving medicines (latest quarter)",
         stakeholder="CSCO", olap="Ranking (Bottom-N)",
         question="Which 10 medicines sold the fewest packs in the latest quarter?",
         base=f"""SELECT m.med_name, m.med_class, m.cold_chain, sum(f.qty_sold) packs
                  FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_medicine m USING (med_key)
                  WHERE d.year || '-' || d.quarter = {LAST_Q}
                  GROUP BY ALL ORDER BY packs ASC LIMIT 10""",
         optimized="""SELECT med_name, med_class, cold_chain, qty_sold FROM sdi.agg_medicine_velocity
                      WHERE year_quarter = (SELECT max(year_quarter) FROM sdi.agg_medicine_velocity)
                      ORDER BY rank_slow LIMIT 10"""),
    dict(name="Q05_forecast_vs_actual", widget="Hospital forecast vs actual consumption",
         stakeholder="CSCO", olap="Variance (forecast vs actual) by hospital x month",
         question="Consumed / forecast ratio for every hospital and month.",
         base="""SELECT d.month_start_date, h.hosp_name, sum(f.qty_consumed) consumed, sum(f.qty_forecast) forecast,
                        round(sum(f.qty_consumed)/sum(f.qty_forecast),3) ratio
                 FROM sdi.fact_demand f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_hospital h USING (hosp_key)
                 GROUP BY 1,2 ORDER BY 1,2""",
         optimized="""SELECT a.month_key, h.hosp_name, sum(qty_consumed), sum(qty_forecast),
                             round(sum(qty_consumed)/sum(qty_forecast),3)
                      FROM sdi.agg_demand_vs_sales a JOIN sdi.dim_hospital h USING (hosp_key) GROUP BY 1,2 ORDER BY 1,2"""),
    dict(name="Q06_unfulfilled_demand", widget="Unfulfilled demand by hospital type and medicine",
         stakeholder="CSCO / CFO", olap="Dice + exception",
         question="Where is hospital demand not met, and what is it worth?",
         base="""SELECT h.hosp_type, m.med_name, sum(f.qty_unfulfilled) packs, sum(f.unfulfilled_value_usd) value_usd,
                        sum(f.stockout_days) stockout_days
                 FROM sdi.fact_demand f JOIN sdi.dim_hospital h USING (hosp_key) JOIN sdi.dim_medicine m USING (med_key)
                 GROUP BY 1,2 HAVING sum(f.qty_unfulfilled) > 0 ORDER BY value_usd DESC""",
         optimized=None),
    dict(name="Q07_demand_vs_sales_hospital", widget="Hospital consumption vs hospital-pharmacy sales",
         stakeholder="COO", olap="Drill-across (two facts, conformed Date + Hospital)",
         question="For each hospital and month, consumption (FactDemand) vs packs sold by its pharmacy (FactSales).",
         base="""WITH dem AS (SELECT date_key mk, hosp_key, sum(qty_consumed) consumed FROM sdi.fact_demand GROUP BY 1,2),
                      sal AS (SELECT CAST(strftime(d.month_start_date,'%Y%m%d') AS INT) mk, f.hosp_key, sum(qty_sold) sold
                              FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) WHERE f.hosp_key > 0 GROUP BY 1,2)
                 SELECT h.hosp_name, dem.mk, consumed, COALESCE(sold,0) sold
                 FROM dem LEFT JOIN sal USING (mk, hosp_key) JOIN sdi.dim_hospital h USING (hosp_key) ORDER BY 1,2""",
         optimized="""SELECT h.hosp_name, month_key, sum(qty_consumed), sum(hosp_pharmacy_qty_sold)
                      FROM sdi.agg_demand_vs_sales a JOIN sdi.dim_hospital h USING (hosp_key) GROUP BY 1,2 ORDER BY 1,2"""),
    dict(name="Q08_rollup_geography", widget="Net sales by Region > State > City",
         stakeholder="CSCO", olap="Roll-up (city -> state -> region)",
         question="How do net sales roll up from city to state to region?",
         base="""SELECT l.region, l.state, l.city, sum(f.net_sales_usd) net_sales
                 FROM sdi.fact_sales f JOIN sdi.dim_location l USING (location_key)
                 GROUP BY ROLLUP (l.region, l.state, l.city) ORDER BY 1,2,3""",
         optimized="""SELECT l.region, l.state, l.city, sum(a.net_sales_usd)
                      FROM sdi.agg_sales_month a JOIN sdi.dim_location l USING (location_key)
                      GROUP BY ROLLUP (l.region, l.state, l.city) ORDER BY 1,2,3"""),
    dict(name="Q09_drilldown_time", widget="Year > Quarter > Month drill-down",
         stakeholder="CFO", olap="Drill-down (year -> quarter -> month)",
         question="Diabetes medicine net sales by year, then quarter, then month.",
         base="""SELECT d.year, d.quarter, d.month_name, sum(f.net_sales_usd) net_sales
                 FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_medicine m USING (med_key)
                 WHERE m.disease_cat = 'Diabetes Mellitus'
                 GROUP BY GROUPING SETS ((d.year),(d.year,d.quarter),(d.year,d.quarter,d.month_name)) ORDER BY 1,2,3""",
         optimized=None),
    dict(name="Q10_pivot_disease_pharmacy", widget="Disease category x pharmacy type matrix",
         stakeholder="CCO", olap="Pivot",
         question="Net sales of each disease category with pharmacy types as columns.",
         base="""PIVOT (SELECT m.disease_cat, p.pharm_type, f.net_sales_usd
                        FROM sdi.fact_sales f JOIN sdi.dim_medicine m USING (med_key) JOIN sdi.dim_pharmacy p USING (pharm_key))
                 ON pharm_type USING sum(net_sales_usd) ORDER BY disease_cat""",
         optimized=None),
    dict(name="Q11_yoy_growth", widget="Sales growth % (month vs same month last year)",
         stakeholder="CFO", olap="Period-over-period",
         question="Year-on-year growth of net sales for every month.",
         base="""WITH m AS (SELECT d.year, d.month_no, sum(f.net_sales_usd) net FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) GROUP BY 1,2)
                 SELECT year, month_no, net, round(100*(net/lag(net) OVER (PARTITION BY month_no ORDER BY year)-1),2) yoy_pct
                 FROM m ORDER BY 1,2""",
         optimized="""WITH m AS (SELECT year(month_start_date) y, month(month_start_date) mo, sum(net_sales_usd) net FROM sdi.agg_sales_month GROUP BY 1,2)
                      SELECT y, mo, net, round(100*(net/lag(net) OVER (PARTITION BY mo ORDER BY y)-1),2) FROM m ORDER BY 1,2"""),
    dict(name="Q12_fill_rate_cold_chain", widget="Order fill rate of cold-chain medicines",
         stakeholder="CSCO", olap="Slice (Cold.Chain = Y) + roll-up by hospital type",
         question="For cold-chain medicines, what is the order fill rate (received / requested) each month, by hospital type?",
         base="""SELECT d.month_start_date, h.hosp_type, round(100.0*sum(f.qty_received)/sum(f.qty_requested),2) fill_rate_pct
                 FROM sdi.fact_demand f JOIN sdi.dim_medicine m USING (med_key) JOIN sdi.dim_hospital h USING (hosp_key)
                 JOIN sdi.dim_date d USING (date_key) WHERE m.cold_chain = 'Y' GROUP BY 1,2 ORDER BY 1,2""",
         optimized=None),
    dict(name="Q13_class_quarter_region", widget="Medicine class sales by quarter, region and pharmacy type",
         stakeholder="CCO", olap="Dice (4 dimensions)",
         question="Which medicine classes earned the most net sales and margin each quarter, by region and pharmacy type?",
         base="""SELECT d.year, d.quarter, l.region, p.pharm_type, m.med_class, sum(f.net_sales_usd) net, sum(f.gross_margin_usd) gm
                 FROM sdi.fact_sales f JOIN sdi.dim_date d USING (date_key) JOIN sdi.dim_location l USING (location_key)
                 JOIN sdi.dim_pharmacy p USING (pharm_key) JOIN sdi.dim_medicine m USING (med_key)
                 GROUP BY ALL ORDER BY net DESC LIMIT 50""",
         optimized=None),
    dict(name="Q14_demand_vs_sales_medicine", widget="Hospital demand vs pharmacy sales per medicine",
         stakeholder="COO / CSCO", olap="Drill-across (conformed Date + Medicine)",
         question="For each medicine and month, how do forecast and consumption compare with packs sold?",
         base="""WITH dem AS (SELECT m.med_name, f.date_key mk, sum(qty_forecast) forecast, sum(qty_consumed) consumed
                              FROM sdi.fact_demand f JOIN sdi.dim_medicine m USING (med_key) GROUP BY 1,2),
                      sal AS (SELECT m.med_name, CAST(strftime(d.month_start_date,'%Y%m%d') AS INT) mk, sum(f.qty_sold) sold
                              FROM sdi.fact_sales f JOIN sdi.dim_medicine m USING (med_key) JOIN sdi.dim_date d USING (date_key) GROUP BY 1,2)
                 SELECT * FROM dem LEFT JOIN sal USING (med_name, mk) ORDER BY 1,2""",
         optimized=None),
    dict(name="Q15_returns_by_channel", widget="Return rate by pharmacy type and channel",
         stakeholder="CCO", olap="Slice on degenerate attributes",
         question="Which pharmacy type and sales channel have the highest return rate?",
         base="""SELECT p.pharm_type, f.sales_channel, round(100.0*sum(f.qty_returned)/sum(f.qty_sold),3) return_rate_pct
                 FROM sdi.fact_sales f JOIN sdi.dim_pharmacy p USING (pharm_key) GROUP BY 1,2 ORDER BY 3 DESC""",
         optimized=None),
    dict(name="Q16_scd2_as_was_vs_as_is", widget="Rx vs OTC sales: history as-was vs restated",
         stakeholder="CFO", olap="SCD Type 2 point-in-time vs current",
         question="Net sales by prescription class using the class at the time of sale vs today's class.",
         base="""SELECT d.year, m.presc_class AS class_as_was, cur.presc_class AS class_as_is, sum(f.net_sales_usd) net
                 FROM sdi.fact_sales f JOIN sdi.dim_medicine m USING (med_key)
                 JOIN sdi.dim_medicine cur ON cur.med_id = m.med_id AND cur.is_current = 'Y'
                 JOIN sdi.dim_date d USING (date_key) GROUP BY 1,2,3 ORDER BY 1,2,3""",
         optimized=None),
]

BROKEN_QUERY = dict(
    name="Q99_negative_test", widget="(negative test) query with a wrong column name",
    stakeholder="QA", olap="n/a", question="Proves that failed queries are caught and counted.",
    base="SELECT sum(net_sales_amount) FROM sdi.fact_sales", optimized=None)
