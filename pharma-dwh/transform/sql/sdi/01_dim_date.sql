-- DimDate: one row per day, 2023-01-01 .. 2030-12-31 (Type 0, generated once).
-- Role-playing: Sales Date (FactSales) and Demand Month (FactDemand).
CREATE SCHEMA IF NOT EXISTS sdi;
CREATE TABLE IF NOT EXISTS sdi.dim_date AS
SELECT
    CAST(strftime(d, '%Y%m%d') AS INTEGER)        AS date_key,        -- Date.Key
    CAST(date_trunc('month', d) AS DATE)          AS month_start_date, -- Month.StartDate
    CAST(month(d) AS INTEGER)                     AS month_no,         -- Month.No
    monthname(d)                                  AS month_name,       -- Month.Name
    'Q' || quarter(d)                             AS quarter,          -- Quarter
    CAST(year(d) AS INTEGER)                      AS year,             -- Year
    CAST(dayofmonth(last_day(d)) AS INTEGER)      AS days_in_month,    -- Days.InMonth
    CAST(d AS DATE)                               AS full_date         -- Full.Date
FROM range(DATE '2023-01-01', DATE '2031-01-01', INTERVAL 1 DAY) t(d);
