from pyspark import pipelines as dp 

BASE_DIR = "/Volumes/marathos/default/raw"

schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{BASE_DIR}/data/TWO_CENTURIES_OF_UM_RACES.csv")
    .schema
)

@dp.table(name = "marathos.bronze.bronze_ultra_marathon", 
          comment = "Raw ultra marathon data for Marathos - bronze layer", 
          table_properties = {
            "delta.columnMapping.mode": "name",
           "delta.minReaderVersion": "2",
           "delta.minWriterVersion": "5"
          })
def raw_ultra_marathon():
    return spark.readStream.format("csv").options(header  =True, encoding = "latin1").schema(schema).load(f"{BASE_DIR}/data")