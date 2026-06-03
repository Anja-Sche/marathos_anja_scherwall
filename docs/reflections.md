# Reflections

QUESTIONS TO STAKEHOLDERS

THE TYDLIGHET OF USING MEDALLION ARCHITECTURE

## Running the pipeline

In earlier projects I have worked with smaller sizes of datasets. Even though I understood it would take time with more data, it was hard to understand that it could take several minutes for the pipeline to run through all the layers (bronze, silver, gold)

## Cleaning
When cleaning the data you see new things for every new step and query. The silver layer took the longest time because there is where you did the cleaning, and where you discover more and more things to be cleaned. 

when moving on to gold layer and later the dashboard and genie, there were things surfacing that needed alteration or more cleaning. This in combination with the pipeline taking time to run got frustrating at times.

## Hardest part

The hardest part of this project was to decide when to move on and stop the cleaning due to time limit. Things like country codes for example where I could have mapped SVE to Sweden manually. 