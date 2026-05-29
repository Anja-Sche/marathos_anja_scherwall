from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case
from pyspark.sql import functions as sf
from pyspark.sql.functions import col


@dep.table(
    name="marathos.silver.silver_ultra_marathon_obt",
    comment="Cleaned ultra marathon data and country code data, joined to obt (one big table) for Marathos - silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
    },
)
def cleaned_marathos():
    df = spark.sql("FROM STREAM supply_chain.bronze.bronze_ultra_marathon")
    df = rename_columns_to_snake_case(df)

    return (
        df.withColumn(
            "event_distance_length_value",
            sf.regexp_extract(col("event_distance_length"), r"^([0-9]+)", 1),
        )
        .withColumn(
            "distance_length_type",
            sf.regexp_extract(col("event_distance_length"), r"([a-zA-Z]+)", 1),
        )
        .withColumn(
            "athlete_performance_value_km",
            sf.regexp_extract(col("athlete_performance"), r"([0-9.,]+)\s*km", 1),
        )
        .withColumn(
            "athlete_performance_value_h",
            sf.regexp_extract(col("athlete_performance"), r"([0-9:;]+)\s*h", 1),
        )
        .withColumn(
            "event_country_code",
            sf.regexp_extract(col("event_name"), r"\(([a-zA-Z]+)\)", 1),
        )
        .withColumn(
            "name_of_event",
            sf.regexp_extract(col("event_name"), r"(.*[a-zA-Z]+)\s*\(", 1),
        )
    )
