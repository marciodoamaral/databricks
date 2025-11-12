import dlt
from pyspark.sql.types import StructType, StringType, LongType, MapType

# Define CDC schema (Debezium format)
cdc_schema = StructType() \
  .add("op", StringType()) \
  .add("before", MapType(StringType(), StringType())) \
  .add("after", MapType(StringType(), StringType())) \
  .add("source", StructType()
    .add("ts_ms", LongType())
    .add("schema", StringType())
    .add("table", StringType())) \
  .add("ts_ms", LongType())

@dlt.table(
  name="raw_cdc_events",
  comment="Raw CDC events from Debezium (JSON)",
  table_properties={
    "quality": "raw",
    "layer": "bronze",
    "pipelines.autoOptimize.zOrderCols": "source.table, op"
  }
)

def raw_cdc_events():
  return (
    spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .schema(cdc_schema)
      .option("badRecordsPath", "s3a://dlt-logs-bad-records/raw_cdc/")
      .option("cloudFiles.schemaEvolutionMode", "rescue")  # Key for schema evolution
      .load("s3a://cdc-source-data/financial/")
  )
