@dlt.table
def exchange_rates():
  return spark.createDataFrame(
    [("USD", 1.0), ("EUR", 0.92), ("GBP", 0.78)],
    ["currency", "rate_to_eur"]
  )

@dlt.table
def enriched_transactions():
  txns = dlt.read("transactions_scd2")
  rates = dlt.read("exchange_rates")
  
  return (
    txns.join(rates, on="currency", how="left")
      .withColumn("amount_eur", col("amount") * col("rate_to_eur"))
      .withColumn("risk_score", 
                  when(col("amount") > 1000, 0.8)
                  .otherwise(0.3))
      .select(
        "transaction_id", "account_id", "amount", "currency",
        "amount_eur", "risk_score", "start_ts", "end_ts"
      )
  )