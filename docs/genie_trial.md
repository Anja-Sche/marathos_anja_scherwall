# Test on databricks genie(AI)

To make sure that genie makes correct and trustworthy awnsers, I asked a few questions and made SQL querys to check the correctness of the awnswer.\
This genie is connected to two marts (focused subset of data), the following questions have been asked on both marts. Here you will se questions connected to the distance mart.

More questions were asked to genieand the conclution afte a short investigation, geni is reliable for this data. It is always better to be specific when asking a question to get a more directed awnser.
## Question 1

### Question: 
What is the distribution (min, max, average, median) of event distance (km) in the mart_distance table?


### Genie awnser: 

Event Distance Distribution
The event distances in the mart_distance table range from 44 km to 370 km, with an average of 78.76 km and a median of 64.37 km.


### SQL query:
 ``` sql
SELECT
    MIN(event_distance_km),
    MAX(event_distance_km),
    AVG(event_distance_km),
    MEDIAN(event_distance_km)
FROM mart_distance;
```

### Query result:
min 44	\
max	370	\
avg 78.75902348342737	\
median 64.37

## Question 2

### Question: 
How many events are in the top 5 countries hosting events? one for martdistance and one for mart lenght?

### Genie awnser: 

Top 5 Countries Hosting Events: \
United States: 21,186 events \
France: 5,224 events \
United Kingdom: 3,488 events \
Germany: 3,103 events \
Australia: 2,525 events


### SQL query:
 ``` sql
SELECT
    event_country,
    COUNT (event_name) AS amount_of_events
FROM mart_distance
GROUP BY 
    event_country
ORDER BY amount_of_events DESC
LIMIT 5;
```

### Query result:
event_country	amount_of_events\
United States   21186\
France          5224\
United Kingdom  3488\
Germany	        3103\
Australia       2525

## Question 3

### Question: 
What is the average number of races per year? If you look at mart_distance?

### Genie awnser: 

The average number of races per year in mart_distance is 476 races across the entire dataset (1837–2022).

Modern trends (2015–2022)
For the recent years, the average is significantly higher: 4,318 races per year (2015–2022). This shows a strong growth in the number of ultramarathon events in modern times.


### SQL query:
 ``` sql
SELECT
    ROUND(AVG(number_of_races), 2) AS avg_races_per_year
FROM (
    SELECT
        YEAR(start_date) AS year,
        COUNT(DISTINCT event_name) as number_of_races
FROM mart_distance
GROUP BY 
    YEAR(start_date)
);


SELECT
    ROUND(AVG(number_of_races), 2) AS avg_races_per_year
FROM (
    SELECT
        YEAR(start_date) AS year,
        COUNT(DISTINCT event_name) as number_of_races
FROM mart_distance
WHERE YEAR(start_date) >= 2015
GROUP BY 
    YEAR(start_date)
);
```

### Query result:
Avg races per year  476.01  (full data)

Avg races per year  4317.75 (2015-2022)

