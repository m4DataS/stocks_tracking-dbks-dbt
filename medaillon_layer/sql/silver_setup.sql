-- One-time setup for Silver Layer with CDF
-- Catalog: workspace, Schema: finance_tracking_stocks

USE CATALOG workspace;
USE SCHEMA finance_tracking_stocks;

-- 1. Control Table (One-Time Setup)
-- Tracks incremental state per ticker
-- create the table
CREATE TABLE IF NOT EXISTS pipeline_control (
  ticker              STRING PRIMARY KEY,
  last_ingestion_time TIMESTAMP,
  last_cdf_version    BIGINT,
  last_run_time       TIMESTAMP,
  backfill_active     BOOLEAN
)
USING DELTA;
--- alter table properties to allow defining default values
ALTER TABLE pipeline_control SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');
--- set default values properties for the table : def values allow to pass only the primary key
ALTER TABLE pipeline_control ALTER COLUMN last_cdf_version SET DEFAULT 0;
ALTER TABLE pipeline_control ALTER COLUMN backfill_active SET DEFAULT false;

-- Seed initial ticker
-- (Checking if ticker already exists before inserting to avoid primary key violation on re-runs)
MERGE INTO pipeline_control AS tgt
USING (SELECT 'GOOGL' AS ticker UNION ALL SELECT 'AAPL' AS ticker) AS src
ON tgt.ticker = src.ticker
WHEN NOT MATCHED THEN INSERT (ticker) VALUES (src.ticker);

-- 2. Per-Ticker Silver Tables (GOOGL)
CREATE TABLE IF NOT EXISTS silver_GOOGL (
  ticker           STRING,
  timestamp        TIMESTAMP,
  interval         STRING,
  open             DECIMAL(38,10),
  high             DECIMAL(38,10),
  low              DECIMAL(38,10),
  close            DECIMAL(38,10),
  adj_close        DECIMAL(38,10),
  volume           BIGINT,
  first_seen_time  TIMESTAMP,
  last_update_time TIMESTAMP
)
USING DELTA
PARTITIONED BY (interval);

-- Enable CDF on silver_GOOGL
ALTER TABLE silver_GOOGL 
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 3. Create/Initialize History Table for GOOGL
CREATE TABLE IF NOT EXISTS silver_GOOGL_cdf 
USING DELTA 
AS SELECT *, current_timestamp() as _event_time FROM (
  SELECT * FROM table_changes('silver_GOOGL', 0)
) WHERE 1=0;
