import dlt
from pyspark.sql.functions import when, current_timestamp, lit, col

@dlt.table(
  comment="SCD Type 2 table for account history",
  table_properties={"quality": "silver", "layer": "silver"}
)

def accounts_scd2():
  parsed_df = dlt.read("parsed_cdc")
  account_changes = parsed_df.filter("table_name = 'accounts'")
  
  return (
    account_changes
      .withColumn("current", when(col("op") == "d", False).otherwise(True))
      .withColumn("start_ts", col("change_timestamp"))
      .withColumn("end_ts", 
                  when(col("op") == "d", col("change_timestamp"))
                  .otherwise(lit("9999-12-31")))
      .withColumn("version", 
                  when(col("op") == "c", 1)
                  .otherwise(current_timestamp().cast("long")))
      .select(
        "after_data.account_id", 
        "after_data.balance", 
        "after_data.status",
        "current", "start_ts", "end_ts", "version"
      )
      .filter("op != 'd' OR before_data.account_id IS NOT NULL")
  )