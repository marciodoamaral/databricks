def log_pipeline_event(event_type, message):
  log_df = spark.createDataFrame([{
    "event_type": event_type,
    "message": message,
    "timestamp": current_timestamp()
  }])
  dlt.write(log_df, mode="append", table_name="pipeline_logs")