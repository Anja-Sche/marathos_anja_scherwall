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