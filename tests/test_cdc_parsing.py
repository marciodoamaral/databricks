def test_valid_operation():
  df = spark.table("parsed_cdc")
  valid_ops = df.filter("op IN ('c','u','d')")
  assert valid_ops.count() == df.count()