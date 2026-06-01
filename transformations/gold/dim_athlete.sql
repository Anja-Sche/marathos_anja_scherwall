CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
COMMENT "Athlete table for Marathos to access information about athletes- gold layer" AS

SELECT
    athlete_id,
    MAX_BY(athlete_year_of_birth, date_id) AS year_of_birth,
    MAX_BY(athlete_gender, date_id) AS athlete_gender,
    MAX_BY(athlete_country_name, date_id) AS athlete_country
FROM 
    marathos.silver.silver_ultra_marathon_obt
GROUP BY
    athlete_id
ORDER BY athlete_id ASC;