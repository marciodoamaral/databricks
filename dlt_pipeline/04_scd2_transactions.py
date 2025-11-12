@dlt.table
def transactions_scd2():
  parsed_df = dlt.read("parsed_cdc")
  txn_changes = parsed_df.filter("table_name = 'transactions'")
  
  return (
    txn_changes
      .withColumn("current", when(col("op") == "d", False).otherwise(True))
      .withColumn("start_ts", col("change_timestamp"))
      .withColumn("end_ts", 
                  when(col("op") == "d", col("change_timestamp"))
                  .otherwise(lit("9999-12-31")))
      .select(
        "after_data.transaction_id",
        "after_data.account_id",
        "after_data.amount",
        "after_data.currency",
        "current", "start_ts", "end_ts"
      )
      .filter("op != 'd' OR before_data.transaction_id IS NOT NULL")
  )