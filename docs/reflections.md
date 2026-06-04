# Reflections
The first reflection or thought I had was that stakeholders play a very big part when building a pipeline and cleaning data. Being able to ask what they want with the data and what is important to clean/keep. When cleaning data and without having a clear view of what the end result should be, it will be hard to move on and set boundaries.

## Medallion architecture
After working with medallion architecture it gets clear why it is a good standard. You can always go back to the raw data or change how the data should be cleaned without losing anything. While that is a big plus, the different steps are also clearer. Raw data goes into bronze, clean data into silver and the model structure/tables go into gold.

## Running the pipeline

In earlier projects I have worked with smaller sizes of datasets. Even though I understood it would take time with more data, it was hard to understand that it could take several minutes for the pipeline to run through all the layers (bronze, silver, gold)

## Cleaning
When cleaning the data you see new things for every new step and query. The silver layer took the longest time because there is where you did the cleaning, and where you discover more and more things to be cleaned. 

when moving on to gold layer and later the dashboard and genie, there were things surfacing that needed alteration or more cleaning. This in combination with the pipeline taking time to run got frustrating at times.

When choosing how to create new athlete ID i initiailly thought of using the old athlete ID in combination with birth year, athlete country and gender. After looking at the new dataset I realized that an athlete can get different IDs in different races. The issue arises if two athletes are from the same country, with same age and gender. If for email or name would had been included in the data, the problem would not arise. **For this dataset tho I chose to use the old athlete ID due to the change in procentage between male and female got very missleading.**

## Hardest part

The hardest part of this project was to decide when to move on and stop the cleaning due to time limit. Things like country codes for example where I could have mapped SVE to Sweden manually. 
