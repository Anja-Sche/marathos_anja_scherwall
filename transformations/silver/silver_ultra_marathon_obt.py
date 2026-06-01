from pyspark import pipelines as dp
from utils.utils import (
    rename_columns_to_snake_case,
    remove_unclean_dl_performance,
    clean_event_distance_length,
    calculate_performance_h,
    extract_country_code_event_name,
    split_dates,
    calculate_age,
    valid_age,
    calculate_avg_speed,
    valid_avg_speed,
    create_country_and_code,
    join_country_name,
    null_to_unknown_strings_types,
    validate_gender_category
)
from pyspark.sql import functions as sf


# create table
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
    df = df.filter(validate_gender_category())

    df = calculate_avg_speed(df)
    df = valid_avg_speed(df)

    df_countries = create_country_and_code(df_country, df_country_old)
    df = join_country_name(df, df_countries, "event_country_code", "event_country")
    df = join_country_name(df, df_countries, "athlete_country", "athlete_country_name")
    df = null_to_unknown_strings_types(df)

    df = (
        df.withColumn(
            "date_id",
            sf.abs(sf.xxhash64(sf.concat_ws("||", sf.col("start_date"), sf.col("end_date"))))
        ).withColumn(
            "event_id",
            sf.abs(sf.xxhash64(sf.concat_ws("||", sf.col("name_of_event"), sf.col("start_date"))))
        ).withColumn(
            "result_id",
            sf.abs(sf.xxhash64(sf.concat_ws("||", sf.col("athlete_id"), sf.col("event_id"))))
        )
        .drop(
            "event_distance_length",
            "athlete_performance",
            "event_name",
            "athlete_average_speed",
            "event_dates",
            "event_country_code",
            "athlete_country",
        )
    )
    return df
