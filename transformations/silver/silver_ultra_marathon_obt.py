from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case, remove_unclean_dl_performance, clean_vent_distance_length, calculate_performance_h
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
    df = remove_unclean_dl_performance(df)
    df = clean_vent_distance_length(df)
    df = calculate_performance_h(df)

    return (
        df.withColumn(
            "event_country_code",
            sf.regexp_extract(col("event_name"), r"\(([a-zA-Z]+)\)", 1),
        )
        .withColumn(
            "name_of_event",
            sf.regexp_extract(col("event_name"), r"(.*[a-zA-Z]+)\s*\(", 1),
        )
    )
