import re
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.functions import col, coalesce, lit, StringType
from functools import reduce

def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]+", "_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)

def remove_unclean_dl_performance(df):
    return df.filter(
        (
            sf.lower(sf.col("event_distance_length")).rlike("(km|mi)$")
            & sf.lower(sf.col("athlete_performance")).rlike("h$")
            & ~sf.lower(sf.col("athlete_performance")).rlike("d")
        )
        | (
            sf.lower(sf.col("event_distance_length")).rlike("h$")
            & sf.lower(sf.col("athlete_performance")).rlike("km$")
        )
        | (
            sf.lower(sf.col("athlete_performance")).rlike("km$")
            & sf.lower(sf.col("event_distance_length")).rlike("h$")
        )
        | (
            sf.lower(sf.col("athlete_performance")).rlike("h$")
            & sf.lower(sf.col("event_distance_length")).rlike("(km|mi)$")
        )
    )

def clean_event_distance_length(df):
    df = (
        df.withColumn(
            "event_distance_length_value",
            sf.regexp_extract(col("event_distance_length"), r"^([0-9]+)", 1),
        )
        .withColumn(
            "event_distance_length_type",
            sf.regexp_extract(col("event_distance_length"), r"([a-zA-Z]+)", 1),
        )
        .withColumn(
            "event_distance_length_value",
            sf.when(
                sf.col("event_distance_length_type") == "mi",
                sf.col("event_distance_length_value").cast("double") * 1.609344,
            ).otherwise(sf.col("event_distance_length_value").cast("double")),
        )
        .withColumn(
            "event_distance_length_type",
            sf.when(
                sf.col("event_distance_length_type") == "mi", sf.lit("km")
            ).otherwise(sf.col("event_distance_length_type")),
        )
    )
    return df

def calculate_performance_h(df):
    df = (
        df.withColumn(
            "athlete_performance_value",
            sf.regexp_extract(col("athlete_performance"), r"([0-9,.;:]+)\s*", 1),
        )
        .withColumn(
            "athlete_performance_type",
            sf.regexp_extract(col("athlete_performance"), r"([a-zA-Z]+)", 1),
        )
        .withColumn(
            "athlete_performance_value",
            sf.when(
                sf.col("athlete_performance_value").contains(":"),
                sf.round(((
                            sf.expr(
                                "try_cast(substring_index(athlete_performance_value, ':', 1) as int)")* 3600
                            + sf.expr("try_cast(substring_index(substring_index(athlete_performance_value, ':', 2), ':', -1) as int)"
                            )* 60
                            + sf.expr("try_cast(substring_index(athlete_performance_value, ':', -1) as int)"
                            )
                            )/ 3600),5,).cast("double"),
            ).otherwise(sf.expr("try_cast(athlete_performance_value as double)")),
        )
    )
    return df

def extract_country_code_event_name(df):
    df = df.withColumn(
        "name_of_event",
        sf.regexp_extract(col("event_name"), r"(.*[a-zA-Z]+)\s*\(", 1)
    ).withColumn(
        "event_country_code",
        sf.regexp_extract(col("event_name"), r"\(([a-zA-Z]+)\)", 1)
    )
    return df

def split_dates(df):
    df = df.withColumn(
    "event_start_date",
    sf.when(
        sf.col("event_dates").rlike(r"^\d{2}\.-\d{2}\."),
        sf.concat(
            sf.substring(sf.col("event_dates"), 1, 3),
            sf.substring(sf.col("event_dates"), 8, 8))
    ).when(
        sf.col("event_dates").rlike(r"^\d{2}\.\d{2}\.\-"),
        sf.concat(
            sf.substring(sf.col("event_dates"), 1, 6),  
            sf.substring(sf.col("event_dates"), -4, 4)  )
    ).otherwise(
        sf.when(
            sf.col("event_dates").contains("-"),
            sf.split(sf.col("event_dates"), "-").getItem(0)
        ).otherwise(sf.substring(sf.col("event_dates"), 1, 10))
    )
    ).withColumn(
        "event_start_date",
        sf.try_to_date(sf.col("event_start_date"), "dd.MM.yyyy")
    ).withColumn(
        "event_end_date",
        sf.when(
            sf.col("event_dates").contains("-"),
            sf.regexp_extract(sf.col("event_dates"), r"\-([0-9,.;:]+)\s*", 1)
        ).otherwise(sf.col("event_dates"))
    ).withColumn(
        "event_end_date",
        sf.try_to_date(sf.col("event_end_date"), "dd.MM.yyyy")
    )
    return df

def calculate_age(df):
    df = df.withColumn(
        "athlete_age",
        sf.col("year_of_event") - sf.col("athlete_year_of_birth").cast("int")
    )
    return df

def valid_age(df):
    valid_age = (
        (sf.col("athlete_age") >= 10) & 
        (sf.col("athlete_age") <= 95) 
    )
    return df.filter(valid_age)

def calculate_avg_speed(df):
    df = df.withColumn(
        "athlete_avg_speed",
        sf.round(
            sf.when(
                sf.col("event_distance_length_type") == "km",
                (
                    sf.try_divide(
                        sf.col("event_distance_length_value"),
                        sf.col("athlete_performance_value"),
                    )
                ),
            ).otherwise(
                sf.try_divide(
                    sf.col("athlete_performance_value"),
                    sf.col("event_distance_length_value"),
                )
            ),
            2,
        ),
    )
    return df

def valid_avg_speed(df):
    valid_speed = sf.col("athlete_avg_speed") <= 20
    return df.filter(valid_speed)

def create_country_and_code(df_country, df_country_old):
    def clean_match(df, code_col):
        return (
            df.select("Country", sf.upper(sf.col(code_col)).alias("code"))
            .filter(
                (sf.col("Country").isNotNull()) & (sf.col("Country") != "NA") &
                (sf.col("code").isNotNull()) & (sf.col("code") != "NA")
            )
        )
    dfs = [
        clean_match(df_country, "IOC"),
        clean_match(df_country, "ISO"),
        clean_match(df_country, "FIFA"),
        clean_match(df_country_old, "IOC"),
        clean_match(df_country_old, "ISO"),
        clean_match(df_country_old, "FIFA")
    ] 
    return reduce(DataFrame.union, dfs).distinct()

def join_country_name(df, df_countries_and_codes, country_code_col, new_col_name):
    df = df.join(
        df_countries_and_codes, 
        sf.col(country_code_col) == df_countries_and_codes["code"], 
        how="left"
    )
    df = df.withColumnRenamed("Country", new_col_name)
    
    return df.drop(df_countries_and_codes["code"])

def null_to_unknown_strings_types(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, sf.coalesce(sf.col(field.name), sf.lit("unknown")))
            
    return df
