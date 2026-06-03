import re
from pyspark.sql import functions as sf, DataFrame
from pyspark.sql.functions import col, coalesce, lit, when, lower, upper
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
            lower(col("event_distance_length")).rlike("(km|mi)$")
            & lower(col("athlete_performance")).rlike("h$")
            & ~lower(col("athlete_performance")).rlike("d")
        )
        | (
            lower(col("event_distance_length")).rlike("h$")
            & lower(col("athlete_performance")).rlike("km$")
        )
        | (
            lower(col("athlete_performance")).rlike("km$")
            & lower(col("event_distance_length")).rlike("h$")
        )
        | (
            lower(col("athlete_performance")).rlike("h$")
            & lower(col("event_distance_length")).rlike("(km|mi)$")
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
                when(
                    col("distance_length_type") == "mi",
                    sf.expr("try_cast(distance_length_value as double)") * 1.609344,
                ).otherwise(sf.expr("try_cast(distance_length_value as double)")),
                2,))
        .withColumn(
            "distance_length_type",
            when(col("distance_length_type") == "mi", lit("km")).otherwise(
                col("distance_length_type")
            ))
        .filter(col("distance_length_value").isNotNull())
    )
    return df

"""Validate distance, > 42km (distance of a marathon)"""

def distance_filter(df):
    df = df.filter(
            ((col("distance_length_type") == "km") &
            (col("distance_length_value") > 42)) |
        (col("distance_length_type") == "h")
    )
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
            when(
                col("performance_value").contains(":"),
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
    ).filter(col("performance_value").isNotNull())

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
        "event_dates",
        when(
            (sf.length(col("event_dates")) == 10) & col("event_dates").contains("-"),
            sf.regexp_replace(col("event_dates"), "-", ".")
        ).otherwise(col("event_dates"))
    ).withColumn(
    "start_date",
    when(
        col("event_dates").rlike(r"^\d{2}\.-\d{2}\."),
        sf.concat(
            sf.substring(col("event_dates"), 1, 3),
            sf.substring(col("event_dates"), 8, 8)
        )
    ).when(
        col("event_dates").rlike(r"^\d{2}\.\d{2}\.\-"),
        sf.concat(
            sf.substring(col("event_dates"), 1, 6),  
            sf.substring(col("event_dates"), -4, 4)  
        )
    ).otherwise(
        when(
            col("event_dates").contains("-"),
            sf.split(col("event_dates"), "-").getItem(0)
        ).otherwise(sf.substring(col("event_dates"), 1, 10))
    )
    ).withColumn(
        "start_date",
        sf.try_to_date(col("start_date"), "dd.MM.yyyy")
    ).withColumn(
        "end_date",
        when(
            col("event_dates").contains("-"),
            sf.regexp_extract(col("event_dates"), r"\-([0-9,.;:]+)\s*", 1)
        ).otherwise(col("event_dates"))
    ).withColumn(
        "end_date",
        sf.try_to_date(col("end_date"), "dd.MM.yyyy")
    ).dropna(subset=["start_date", "end_date"])
    return df

"""Calculate and validate age"""

def calculate_age(df):
    df = df.withColumn(
        "athlete_age",
        col("year_of_event") - col("athlete_year_of_birth").cast("int")
    )
    return df

def valid_age(df):
    age_filter = (
        (col("athlete_age") >= 10) & 
        (col("athlete_age") <= 95) 
    )
    return df.filter(age_filter).drop("athlete_age")

"""Validate that gender and age category match, standardize gender name"""

def validate_gender_category(df):
    extracted_letter = sf.regexp_extract(col("athlete_age_category"), r"(^[a-zA-Z]+)\s*", 1)
    
    valid_male = (col("athlete_gender") == "M") & (extracted_letter.isin("M", "MU"))
    vaild_female = (col("athlete_gender") == "F") & (extracted_letter.isin("F", "FU", "W", "WU"))

    df = df.filter(valid_male | vaild_female)
        
    return df

def standardize_gender_name(df):
    return df.withColumn(
        "athlete_gender",
        when(col("athlete_gender") == "M", lit("Male"))
        .otherwise(lit("Female"))
    )

"""Calculate athlete avg speed"""

def calculate_avg_speed(df):
    df = df.filter(
        (col("performance_value") > 0) & (col("distance_length_value") > 0)
        ).withColumn(
            "athlete_avg_speed",
            sf.round(
                when(
                (col("distance_length_type") == "km"),
                sf.try_divide(
                    col("distance_length_value"),
                    col("performance_value")) 
            ).when(
                (col("distance_length_type") == "h"),
                sf.try_divide(
                    col("performance_value"),
                    col("distance_length_value")) 
            ).otherwise(lit(None)), 3 
        )
    )
    return df

def valid_avg_speed(df):
    valid_speed = col("athlete_avg_speed") <= 20
    return df.filter(valid_speed)

"""Join country code/name tables and replace country codes to names for event and athlete"""

def create_country_and_code(df_country, df_country_old):
    def clean_match(df, code_col):
        return (
            df.select("Country", upper(col(code_col)).alias("code"))
            .filter(
                (col("Country").isNotNull()) & (col("Country") != "NA") &
                (col("code").isNotNull()) & (col("code") != "NA")
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
        col(country_code_col) == df_countries_and_codes["code"], 
        how="left"
    )
    df = df.withColumnRenamed("Country", new_col_name)
    
    return df.drop(df_countries_and_codes["code"])

"""Replace null values in strin columns with 'unknown' """

def null_to_unknown_strings_types(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, coalesce(col(field.name), lit("unknown")))
            
    return df
