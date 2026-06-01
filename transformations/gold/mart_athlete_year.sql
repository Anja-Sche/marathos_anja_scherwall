USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_athlete_year
COMMENT "Mart for information about athletes, gender, birth year and year they competed - gold layer" AS

SELECT
    d.year_of_event AS event_year,
    a.year_of_birth AS birth_year,
    a.athlete_gender,
    r.athlete_average_speed,
    d.year_of_event - a.year_of_birth AS athlete_age
FROM
    fct_result r
LEFT JOIN dim_athlete a ON r.athlete_id = a.athlete_id
LEFT JOIN dim_date d ON r.date_id = d.date_id
WHERE 
    (d.year_of_event - a.year_of_birth) >= 10 AND (d.year_of_event - a.year_of_birth) <= 95
GROUP BY 
    event_year,
    birth_year,
    a.athlete_gender,
    r.athlete_average_speed
ORDER BY event_year ASC;