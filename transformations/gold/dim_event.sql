CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
COMMENT "Event table for Marathos to access information about a specific event - gold layer" AS

SELECT
    event_id,
    MAX_BY(name_of_event, date_id) AS event_name,
    MAX_BY(distance_length_value, date_id) AS distance_length_value,
    MAX_BY(distance_length_type, date_id) AS distance_length_type,
    MAX_BY(event_country, date_id) AS event_country,
    MAX_BY(event_number_of_finishers, date_id) AS number_of_finishers
FROM 
    marathos.silver.silver_ultra_marathon_obt
GROUP BY
    event_id
ORDER BY event_id ASC;