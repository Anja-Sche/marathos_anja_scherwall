from pyspark import pipelines as dp 

BASE_DIR = "/Volumes/marathos/default/raw"

schema = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .load(f"{BASE_DIR}/country_data/country_codes_old/country_codes_old.csv")
    .schema
)

@dp.table(name = "marathos.bronze.raw_country_code_old", 
          comment = "Raw country reference data for connecting shortening of countries to the country containing old/historical names and codes - bronze layer", 
          table_properties = {
            "delta.columnMapping.mode": "name",
           "delta.minReaderVersion": "2",
           "delta.minWriterVersion": "5"
          })
def raw_country_reference():
    return spark.readStream.format("csv").options(header  =True, encoding = "latin1", sep=";").schema(schema).load(f"{BASE_DIR}/country_data/country_codes_old")