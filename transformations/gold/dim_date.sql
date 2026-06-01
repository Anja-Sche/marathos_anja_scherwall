CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_date
COMMENT "Date table for Marathos to access year, start date and end date- gold layer" AS

SELECT
    date_id,
    MAX_BY(year_of_event, date_id) AS year_of_event,
    MAX_BY(start_date, date_id) AS start_date,
    MAX_BY(end_date, date_id) AS end_date
FROM 
    marathos.silver.silver_ultra_marathon_obt
GROUP BY
    date_id
ORDER BY date_id ASC;