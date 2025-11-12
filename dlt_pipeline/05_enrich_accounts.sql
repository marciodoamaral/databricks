CREATE OR REFRESH MATERIALIZED VIEW dlt.enriched_accounts AS
SELECT
  a.account_id,
  a.balance,
  a.status,
  a.start_ts,
  a.end_ts,
  CASE 
    WHEN a.balance > 10000 THEN 'PREMIUM'
    WHEN a.balance > 1000 THEN 'REGULAR'
    ELSE 'BASIC'
  END AS customer_tier
FROM STREAM(dlt.accounts_scd2) a