USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_distance
COMMENT "Mart for the events with specific length/times - gold layer" AS

SELECT
    e.event_name,
    e.distance_length_value AS event_distance_km,
    e.event_country,
    e.number_of_finishers,
    d.start_date,
    d.end_date,
    ROUND(AVG(r.performance_value), 2) AS average_time_h,
    ROUND(AVG(r.athlete_average_speed), 2) AS average_speed
FROM
    fct_result r
LEFT JOIN dim_event e ON r.event_id = e.event_id
LEFT JOIN dim_date d ON r.date_id = d.date_id
WHERE 
    e.distance_length_type = "km"
GROUP BY 
    e.event_name, 
    e.distance_length_value, 
    e.event_country,
    e.number_of_finishers,
    d.start_date,
    d.end_date
ORDER BY d.start_date ASC;