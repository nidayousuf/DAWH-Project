-- DimLocation: City + State + Country of every pharmacy (05) and hospital (06).
-- Region is not in the data: it comes from the US Census state -> region list below.
CREATE TABLE IF NOT EXISTS sdi.ref_state AS
SELECT * FROM (VALUES
 ('AL','South'),('AK','West'),('AZ','West'),('AR','South'),('CA','West'),('CO','West'),('CT','Northeast'),
 ('DE','South'),('DC','South'),('FL','South'),('GA','South'),('HI','West'),('ID','West'),('IL','Midwest'),
 ('IN','Midwest'),('IA','Midwest'),('KS','Midwest'),('KY','South'),('LA','South'),('ME','Northeast'),
 ('MD','South'),('MA','Northeast'),('MI','Midwest'),('MN','Midwest'),('MS','South'),('MO','Midwest'),
 ('MT','West'),('NE','Midwest'),('NV','West'),('NH','Northeast'),('NJ','Northeast'),('NM','West'),
 ('NY','Northeast'),('NC','South'),('ND','Midwest'),('OH','Midwest'),('OK','South'),('OR','West'),
 ('PA','Northeast'),('RI','Northeast'),('SC','South'),('SD','Midwest'),('TN','South'),('TX','South'),
 ('UT','West'),('VT','Northeast'),('VA','South'),('WA','West'),('WV','South'),('WI','Midwest'),('WY','West')
) t(state, region);

CREATE TABLE IF NOT EXISTS sdi.dim_location (
    location_key INTEGER,      -- Location.Key
    city         VARCHAR,      -- City
    state        VARCHAR,      -- State
    region       VARCHAR,      -- Region
    country      VARCHAR       -- Country
);
INSERT INTO sdi.dim_location
SELECT (SELECT COALESCE(max(location_key), 0) FROM sdi.dim_location)
         + row_number() OVER (ORDER BY s.state, s.city),
       s.city, s.state, COALESCE(r.region, 'Unknown'), s.country
FROM (
    SELECT DISTINCT trim(pharmacy_city) AS city, upper(trim(pharmacy_state)) AS state,
           upper(trim(country_code)) AS country FROM raw.pharmacy_sales
    UNION
    SELECT DISTINCT trim(city), upper(trim(state)), upper(trim(country_code)) FROM raw.hospital_demand
) s
LEFT JOIN sdi.ref_state r ON r.state = s.state
WHERE NOT EXISTS (SELECT 1 FROM sdi.dim_location l
                  WHERE l.city = s.city AND l.state = s.state AND l.country = s.country);
