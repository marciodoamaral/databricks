def financial_dq_rules():
  return {
    "valid_amount": "amount > 0",
    "valid_currency": "currency IN ('USD', 'EUR', 'GBP')",
    "non_null_id": "transaction_id IS NOT NULL"
  }