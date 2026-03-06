# Stock Market Data Pipeline – Multi-Frequency Ingestion

## Concept

This pipeline implements **multi-frequency ingestion** (also called **multi-granularity ingestion**) where the **same data source** is ingested at **different time intervals**.

Example intervals:

- `1m` – minute data
- `5m`
- `15m`
- `1h`
- `1d` – daily data

This pattern is common in **quantitative finance pipelines** and allows the same dataset to serve **different analytical and ML purposes**.

---

# Architecture

## Bronze Layer (Raw Data)

All ingested data is stored in a **single bronze table**.

Example schema:

| column | description |
|------|-------------|
| ticker | stock symbol |
| timestamp | observation timestamp |
| open | opening price |
| high | highest price |
| low | lowest price |
| close | closing price |
| volume | traded volume |
| interval | data frequency (`1m`, `15m`, `1d`, etc.) |

Example rows:

| ticker | timestamp | close | interval |
|------|------|------|------|
| AAPL | 2026-03-05 15:15 | 182.4 | 15m |
| AAPL | 2026-03-05 | 184.2 | 1d |

### Why the Bronze table is not partitioned

The Bronze layer is intentionally **append-only and unpartitioned**. Its sole responsibility is to land raw data reliably and cheaply — not to optimize query performance.

Partitioning at this stage would introduce unnecessary complexity without meaningful benefit:

- **Data arrives out of order.** Ingestion across multiple intervals (`1m`, `1h`, `1d`) means `timestamp` and `ticker` values are interleaved and non-monotonic, making partition pruning unreliable.
- **Schema is not yet clean.** The presence of duplicate `_str` columns (e.g. `open_str`, `close_str`) signals this is truly raw, unvalidated data. Partitioning should happen on stable, normalized columns — not on a schema that is expected to change.
- **Re-partitioning is expensive.** Any partition scheme applied here would likely need to be redesigned at the Silver layer anyway, wasting ingestion overhead.
- **Bronze is not a query layer.** Downstream Silver jobs consume the full Bronze table in bounded time windows; they are not ad-hoc queries that benefit from partition pruning.

Partitioning is deferred to the **Silver layer**, where data is deduplicated, typed correctly, and `_str` columns are dropped — making query access patterns predictable and stable.

---

## Silver Layer (Cleaned & Partitioned)

> _To be documented._

Recommended partition scheme at Silver:

| partition column | rationale |
|---|---|
| `ticker` | primary filter in all analytical queries |
| `date(timestamp)` | enables efficient time-range pruning |

---

## Gold Layer (Aggregated / ML-ready)

> _To be documented._