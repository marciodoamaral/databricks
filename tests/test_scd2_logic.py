def test_scd2_history():
  result = spark.table("accounts_scd2").filter("account_id = 'A1'").collect()
  assert len(result) == 2
  assert result[0]["end_ts"] == result[1]["start_ts"]