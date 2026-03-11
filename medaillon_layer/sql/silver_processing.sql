-- Main processing logic for Silver Layer (Per Ticker)
-- Catalog: workspace, Schema: finance_tracking_stocks

USE CATALOG workspace;
USE SCHEMA finance_tracking_stocks;

-- Step 3: Job Parameters (Defined in Databricks Job Widgets)
-- These should be set via dbutils.widgets in a notebook, or as job parameters.
-- For a raw SQL file run via SQL Task, we use variable substitution or hardcoded values for testing.
-- SET backfill = 'false';
-- SET backfill_start = '1900-01-01';
-- SET backfill_end = '2100-01-01';
-- SET ticker = 'GOOGL';

-- Step 4.1: Read Control State + Determine Filter
CREATE OR REPLACE TEMP VIEW control_state AS
SELECT *, 
       CASE 
         WHEN '${backfill}' = 'true' THEN NULL  -- backfill ignores last_ingestion_time
         ELSE last_ingestion_time 
       END AS effective_ingestion_filter
FROM pipeline_control 
WHERE ticker = '${ticker}';

-- Step 4.2: Incremental Bronze View (Smart Filter)
CREATE OR REPLACE TEMP VIEW bronze_inc AS
SELECT
  b.ticker, b.timestamp, b.interval,
  b.open, b.high, b.low, b.close, b.adj_close, b.volume,
  -- Quality score (6 business fields)
  (CASE WHEN b.open IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN b.high IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN b.low  IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN b.close IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN b.adj_close IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN b.volume IS NOT NULL THEN 1 ELSE 0 END
  ) AS quality,
  current_timestamp() AS load_time
FROM bronze_yahoo_stocks b
CROSS JOIN control_state c
WHERE b.ticker = '${ticker}'
  AND (
    -- Normal incremental mode
    ('${backfill}' = 'false' AND b.ingestion_time > coalesce(c.effective_ingestion_filter, '1900-01-01'))
    OR
    -- Backfill mode (timestamp range)
    ('${backfill}' = 'true' AND b.timestamp BETWEEN '${backfill_start}' AND '${backfill_end}')
  )
-- Deduplicate source: if multiple records for same period, take highest quality, then latest ingestion
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY b.ticker, b.timestamp, b.interval 
  ORDER BY 
    (CASE WHEN open IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN high IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN low  IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN close IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN adj_close IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN volume IS NOT NULL THEN 1 ELSE 0 END) DESC,
    ingestion_time DESC
) = 1;

-- Step 4.3: MERGE with Quality + Fusion
MERGE INTO silver_${ticker} AS tgt
USING bronze_inc AS src
ON tgt.ticker = src.ticker
AND tgt.interval = src.interval  
AND tgt.timestamp = src.timestamp

WHEN MATCHED AND src.quality >= (
  -- Target quality (recomputed)
  (CASE WHEN tgt.open IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN tgt.high IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN tgt.low IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN tgt.close IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN tgt.adj_close IS NOT NULL THEN 1 ELSE 0 END +
   CASE WHEN tgt.volume IS NOT NULL THEN 1 ELSE 0 END)
) THEN
  UPDATE SET
    open = COALESCE(src.open, tgt.open),
    high = COALESCE(src.high, tgt.high), 
    low = COALESCE(src.low, tgt.low),
    close = COALESCE(src.close, tgt.close),
    adj_close = COALESCE(src.adj_close, tgt.adj_close),
    volume = COALESCE(src.volume, tgt.volume),
    last_update_time = src.load_time

WHEN NOT MATCHED THEN
  INSERT (ticker, timestamp, interval, open, high, low, close, adj_close, volume, first_seen_time, last_update_time)
  VALUES (src.ticker, src.timestamp, src.interval, src.open, src.high, src.low, src.close, src.adj_close, src.volume, src.load_time, src.load_time);

-- Step 4.4: Consume CDF Changes
-- Get current silver version
CREATE OR REPLACE TEMP VIEW current_version AS
SELECT MAX(version) AS ver
FROM (DESCRIBE HISTORY silver_${ticker});

-- Read CDF changes since last processed version
CREATE OR REPLACE TEMP VIEW cdf_changes AS
SELECT *, current_timestamp() as _event_time
FROM table_changes('silver_${ticker}', 
  (SELECT coalesce(last_cdf_version, 0) + 1 FROM control_state), 
  (SELECT ver FROM current_version)
);

-- Note: History table silver_${ticker}_cdf is created in silver_setup.sql
INSERT INTO silver_${ticker}_cdf SELECT * FROM cdf_changes;

-- Step 4.5: Update Control Table
MERGE INTO pipeline_control AS tgt
USING (
  SELECT 
    '${ticker}' AS ticker,
    -- New last_ingestion_time: max seen in this run (or current time)
    GREATEST(
      coalesce((SELECT MAX(ingestion_time) FROM bronze_yahoo_stocks WHERE ticker='${ticker}'), current_timestamp()),
      coalesce((SELECT effective_ingestion_filter FROM control_state), '1900-01-01')
    ) AS new_last_ingestion_time,
    (SELECT ver FROM current_version) AS new_last_cdf_version,
    current_timestamp() AS new_last_run_time,
    '${backfill}'::boolean AS backfill_active
) AS src
ON tgt.ticker = src.ticker
WHEN MATCHED THEN 
  UPDATE SET 
    last_ingestion_time = src.new_last_ingestion_time,
    last_cdf_version = src.new_last_cdf_version,
    last_run_time = src.new_last_run_time,
    backfill_active = src.backfill_active
WHEN NOT MATCHED THEN INSERT *;
