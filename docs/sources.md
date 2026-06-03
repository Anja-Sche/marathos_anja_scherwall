# Sources - data and validation limits

## Data sources
### Main data
The main data is collected from 
[keggle (The big dataset of ultra-marathon running)](https://www.kaggle.com/datasets/aiaiaidavid/the-big-dataset-of-ultra-marathon-running). It includes data about ultra marathon events, distances, dates, athletes and more.

### Country data
To connect the country name with country codes in both event name and athlete conutry I used country data from [github (country_code_ioc_fifa_iso.csv)](https://github.com/prasertcbs/basic-dataset/blob/master/country_code_ioc_fifa_iso.csv). I included a dataset containing ISO, IOC and FIFA codes to get the best accuracy.

Some of the birth years go back to the 1780s and some country names/codes have changed or dissapeared. To match those country codes I created a [CSV-file in Excel](documentation_created_data/country_codes_old.csv) with data from [wikipedia (Comparison of IOC, FIFA, and ISO 3166 country codes)](https://simple.wikipedia.org/wiki/Comparison_of_IOC,_FIFA,_and_ISO_3166_country_codes)



## Validation sources

### Age validation
The age limit for the data going in to silver layer is 10-95 years old.

The yougest person I found a source on to have finished an [ultra marathon was 9yo](https://www.runspirited.com/single-post/2019/10/25/a-13-year-old-ultra-running-champion). The most ages I found was 12-14yo. I choose the age limit of 10 to eliminate the possibility of incorrect data. 

The oldest person I found a source on to have finished a [regular marathon was 100yo](https://www.olympics.com/en/news/who-is-fauja-singh-oldest-indian-origin-british-marathon-runner) and the oldest for running [Badwater Ultramarathon was 80yo](https://www.npr.org/2025/07/16/nx-s1-5467389/meet-the-oldest-runner-to-complete-the-badwater-ultramarathon). This reuslted in the choice of 95yo as the oldest runner in the data. It is between the two ages and gives room for runners in the future setting the record for oldest runner.


### Speed validation (km/h)

The average speed limit for the data going in to silver layer is 20 km/h. 

The [International Association of Ultrarunners](https://iau-ultramarathon.org/iau-records.html) have records on their website for different types of reces, for exampel 100km and 50km. To include different distances low to high, the limit is set from calculating the record for men 50 km. There the record is almost 19 km/h. The limit is a bit higher to give room for future possible records and distances between 43km - 49km.
