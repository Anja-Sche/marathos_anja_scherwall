from pyspark import pipelines as dp 
from utils.utils import rename_columns_to_snake_case, remove_unclean_dl_performance, clean_event_distance_length, calculate_performance_h, extract_country_code_event_name, split_dates, calculate_age, valid_age,calculate_avg_speed, valid_avg_speed, create_country_and_code, join_country_name, null_to_unknown_strings_types
from pyspark.sql import functions as sf
from pyspark.sql.functions import col

#create table
@dp.table(
    name="marathos.silver.silver_ultra_marathon_obt",
    comment="Cleaned ultra marathon data and country code data, joined to obt (one big table) for Marathos - silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def cleaned_marathos():
    df = spark.sql("FROM STREAM marathos.bronze.bronze_ultra_marathon")
    df_country = spark.sql("SELECT * FROM marathos.bronze.raw_country_code")
    df_country_old = spark.sql("SELECT * FROM marathos.bronze.raw_country_code_old")

    df = rename_columns_to_snake_case(df)
    df = remove_unclean_dl_performance(df)
    df = clean_event_distance_length(df)
    df = calculate_performance_h(df)
    df = extract_country_code_event_name(df)
    df = split_dates(df)
    df = calculate_age(df)
    df = valid_age(df)
    df = calculate_avg_speed(df)
    df = valid_avg_speed(df)
    
    df_countries = create_country_and_code(df_country,df_country_old)
    df = join_country_name(df, df_countries, "event_country_code", "event_country")
    df = join_country_name(df, df_countries, "athlete_country", "athlete_country_name")
    df = null_to_unknown_strings_types(df)

    return df
