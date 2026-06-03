USE CATALOG marathos;
USE SCHEMA gold;

CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.mart_athlete_year
COMMENT "Mart for information about athletes, gender, birth year and year they competed - gold layer" AS

SELECT
    a.athlete_id,
    a.year_of_birth AS birth_year,
    a.athlete_gender,
    d.year_of_event - a.year_of_birth AS athlete_age
FROM
    fct_result r
LEFT JOIN dim_athlete a ON r.athlete_id = a.athlete_id
LEFT JOIN dim_date d ON r.date_id = d.date_id
WHERE 
    (d.year_of_event - a.year_of_birth) >= 10 AND (d.year_of_event - a.year_of_birth) <= 95
GROUP BY 
    a.athlete_id,
    birth_year,
    a.athlete_gender,
    d.year_of_event
ORDER BY birth_year ASC;