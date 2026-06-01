CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_result
COMMENT "Result table for Marathos - gold layer" AS

SELECT
    result_id,
    event_id,
    date_id,
    athlete_id,
    performance_value,
    performance_type,
    athlete_avg_speed AS athlete_average_speed,
    athlete_age_category AS age_category,
    athlete_club
FROM 
    STREAM marathos.silver.silver_ultra_marathon_obt;