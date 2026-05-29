import re
from pyspark.sql import functions as sf
from pyspark.sql.functions import to_timestamp, col


def to_snake_case(name):
    name = name.strip().casefold()
    name = re.sub(r"[/]+", "_", name)
    name = re.sub(r"[\s]+", "_", name)
    return name


def rename_columns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)


def remove_unclean_dl_performance(df):
    df = df.filter(
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


def clean_vent_distance_length(df):

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
