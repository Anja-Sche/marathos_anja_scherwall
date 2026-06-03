import re
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.functions import col, coalesce, lit
from pyspark.sql.types import StringType
from functools import reduce

"""Rename columns for easier handeling"""

def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]+", "_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name

def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)

"""Clean and validate event_distance_length"""

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
            "distance_length_value",
            sf.regexp_extract(col("event_distance_length"), r"^([0-9]+)", 1),
        )
        .withColumn(
            "distance_length_type",
            sf.regexp_extract(col("event_distance_length"), r"([a-zA-Z]+)", 1),
        )
        .withColumn(
            "distance_length_value",
            sf.round(
                sf.when(
                    sf.col("distance_length_type") == "mi",
                    sf.expr("try_cast(distance_length_value as double)") * 1.609344,
                ).otherwise(sf.expr("try_cast(distance_length_value as double)")),
                2,))
        .withColumn(
            "distance_length_type",
            sf.when(sf.col("distance_length_type") == "mi", sf.lit("km")).otherwise(
                sf.col("distance_length_type")
            ))
        .filter(sf.col("distance_length_value").isNotNull())
    )
    return df

"""Validate distance, > 42km (distance of a marathon)"""

def distance_filter(df):
    df = df.filter(
            (sf.col("distance_length_type") == "km") &
            (sf.col("distance_length_value") > 42))
    return df

"""Clean performance hour value and create columns performance value/type"""

def calculate_performance_h(df):
    df = (
        df.withColumn(
            "performance_value",
            sf.regexp_extract(col("athlete_performance"), r"([0-9,.;:]+)\s*", 1),
        )
        .withColumn(
            "performance_type",
            sf.regexp_extract(col("athlete_performance"), r"([a-zA-Z]+)", 1),
        )
        .withColumn(
            "performance_value",
            sf.when(
                sf.col("performance_value").contains(":"),
                sf.round(((
                            sf.expr(
                                "try_cast(substring_index(performance_value, ':', 1) as int)")* 3600
                            + sf.expr("try_cast(substring_index(substring_index(performance_value, ':', 2), ':', -1) as int)"
                            )* 60
                            + sf.expr("try_cast(substring_index(performance_value, ':', -1) as int)"
                            )
                            )/ 3600),5,).cast("double"),
            ).otherwise(sf.expr("try_cast(performance_value as double)"))
        )
    ).filter(sf.col("performance_value").isNotNull())

    return df

"""Split event name and event country"""

def extract_country_code_event_name(df):
    df = df.withColumn(
        "name_of_event",
        sf.regexp_extract(col("event_name"), r"(.*[a-zA-Z]+)\s*\(", 1)
    ).withColumn(
        "event_country_code",
        sf.regexp_extract(col("event_name"), r"\(([a-zA-Z]+)\)", 1)
    )
    return df

"""Split dates to start and end date. Include validation"""

def split_dates(df):
    df = df.withColumn(
    "start_date",
    sf.when(
        sf.col("event_dates").rlike(r"^\d{2}\.-\d{2}\."),
        sf.concat(
            sf.substring(sf.col("event_dates"), 1, 3),
            sf.substring(sf.col("event_dates"), 8, 8)
        )
    ).when(
        sf.col("event_dates").rlike(r"^\d{2}\.\d{2}\.\-"),
        sf.concat(
            sf.substring(sf.col("event_dates"), 1, 6),  
            sf.substring(sf.col("event_dates"), -4, 4)  
        )
    ).otherwise(
        sf.when(
            sf.col("event_dates").contains("-"),
            sf.split(sf.col("event_dates"), "-").getItem(0)
        ).otherwise(sf.substring(sf.col("event_dates"), 1, 10))
    )
    ).withColumn(
        "start_date",
        sf.try_to_date(sf.col("start_date"), "dd.MM.yyyy")
    ).withColumn(
        "end_date",
        sf.when(
            sf.col("event_dates").contains("-"),
            sf.regexp_extract(sf.col("event_dates"), r"\-([0-9,.;:]+)\s*", 1)
        ).otherwise(sf.col("event_dates"))
    ).withColumn(
        "end_date",
        sf.try_to_date(sf.col("end_date"), "dd.MM.yyyy")
    ).dropna(subset=["start_date", "end_date"])
    return df

"""Calculate and validate age"""

def calculate_age(df):
    df = df.withColumn(
        "athlete_age",
        sf.col("year_of_event") - sf.col("athlete_year_of_birth").cast("int")
    )
    return df

def valid_age(df):
    age_filter = (
        (sf.col("athlete_age") >= 10) & 
        (sf.col("athlete_age") <= 95) 
    )
    return df.filter(age_filter).drop("athlete_age")

"""Validate that gender and age category match"""

def validate_gender_category(df):
    extracted_letter = sf.regexp_extract(sf.col("athlete_age_category"), r"(^[a-zA-Z]+)\s*", 1)
    
    valid_male = (sf.col("athlete_gender") == "M") & (extracted_letter.isin ("M", "MU"))
    vaild_female = (sf.col("athlete_gender") =="F") & (extracted_letter.isin ("F", "FU", "W", "WU"))

    df = df.withColumn(
        "athlete_gender",
        sf.when(valid_male, sf.lit("Male")).otherwise(sf.col("athlete_gender"))
    ).withColumn(
        "athlete_gender",
        sf.when(vaild_female, sf.lit("Female")).otherwise(sf.col("athlete_gender"))
    )

    df_filtered = df.filter(valid_male | vaild_female)
        
    return df_filtered

"""Calculate athlete avg speed"""

def calculate_avg_speed(df):
    df = df.filter(
        (sf.col("performance_value") > 0) & (sf.col("distance_length_value") > 0)
        ).withColumn(
            "athlete_avg_speed",
            sf.round(
                sf.when(
                (sf.col("distance_length_type") == "km"),
                sf.try_divide(
                    sf.col("distance_length_value"),
                    sf.col("performance_value")) 
            ).when(
                (sf.col("distance_length_type") == "h"),
                sf.try_divide(
                    sf.col("performance_value"),
                    sf.col("distance_length_value")) 
            ).otherwise(sf.lit(None)), 3 
        )
    )
    return df

def valid_avg_speed(df):
    valid_speed = sf.col("athlete_avg_speed") <= 20
    return df.filter(valid_speed)

"""Join country code/name tables and replace country codes to names for event and athlete"""

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

"""Replace null values in strin columns with 'unknown' """

def null_to_unknown_strings_types(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, sf.coalesce(sf.col(field.name), sf.lit("unknown")))
            
    return df
