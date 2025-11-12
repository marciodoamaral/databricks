-- Parse CDC events into structured format
CREATE OR REFRESH MATERIALIZED VIEW dlt.parsed_cdc AS
SELECT
  source.table AS table_name,
  op,
  from_json(before, 'struct<account_id:string, balance:double, status:string>') AS before_data,
  from_json(after, 'struct<account_id:string, balance:double, status:string>') AS after_data,
  CAST(source.ts_ms AS TIMESTAMP) AS change_timestamp,
  CAST(ts_ms AS TIMESTAMP) AS ingest_timestamp,
  _rescued_data  -- Capture schema drift
FROM STREAM(dlt.raw_cdc_events)

-- Apply data quality rules
EXPECT ALL_OR_NONE (
  valid_operation EXPECT op IN ('c', 'u', 'd') ON VIOLATION DROP ROW,
  valid_timestamp EXPECT source.ts_ms IS NOT NULL ON VIOLATION FAIL UPDATE,
  valid_json EXPECT (before IS NOT NULL OR after IS NOT NULL) ON VIOLATION WARN
);