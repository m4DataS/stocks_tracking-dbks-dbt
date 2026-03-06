CREATE TABLE IF NOT EXISTS workspace.finance_tracking_stocks.bronze_yahoo_stocks (
  ticker STRING,
  timestamp TIMESTAMP,
  interval STRING,

  open DECIMAL(38,10),
  open_str STRING,

  high DECIMAL(38,10),
  high_str STRING,

  low DECIMAL(38,10),
  low_str STRING,

  close DECIMAL(38,10),
  close_str STRING,

  adj_close DECIMAL(38,10),
  adj_close_str STRING,

  volume BIGINT,

  ingestion_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (ticker);