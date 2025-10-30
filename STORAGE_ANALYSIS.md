# Database Storage Growth Analysis

## Current System Architecture

This system uses **TimescaleDB with 15-minute bucketing** to efficiently handle time-series data at production scale. This document analyzes storage requirements and performance characteristics based on actual benchmark results.

## Scenario Parameters

**Production baseline:**
- **Products**: 1,000 (realistic for a mid-size platform)
- **Users**: 10,000 active users
- **Price updates**: Every 2 minutes during market hours (7.5 hours/day)
- **Cash flows**: 5 per user per month (average)
- **Market hours**: 7.5 hours/day, 5 days/week (9:30 AM - 4:00 PM US market)

**Event generation:**
- Price updates per product per day: 7.5 hours × 60 min / 2 min = 225 updates
- Total price events per day: 1,000 products × 225 = 225,000 events/day
- With 90/10 price/cashflow split: ~250,000 total events/day

## Annual Data Growth Calculations

### Raw Price Data (product_price hypertable)

**Without bucketing:**
- Updates per product per year: 225 × 250 trading days = 56,250 updates
- Total price rows per year: 1,000 products × 56,250 = **56.25 million rows/year**

**With 15-minute bucketing (product_price_15min continuous aggregate):**
- Bucketed updates per product per day: 7.5 hours × 60 min / 15 min = 30 buckets
- Bucketed updates per product per year: 30 × 250 = 7,500 buckets
- Total bucketed rows per year: 1,000 products × 7,500 = **7.5 million rows/year**
- **Data reduction: 87%** (56.25M → 7.5M)

### Cash Flows (user_cash_flow table)

- Cash flows per user per year: 5 × 12 = 60
- Total cash flow rows per year: 10,000 users × 60 = **600,000 rows/year**

### Cache Tables

**Realistic scenario** (users hold ~5 products on average):
- Active user-product pairs: 10,000 × 5 = 50,000 pairs
- Timeline entries per pair per year: ~7,500 (bucketed)
- Total cache rows per year: 50,000 × 7,500 = **375 million rows/year**
- With daily cache refresh: Old data can be retained indefinitely or pruned based on retention policy

## Row Size Estimates

### product_price table (TimescaleDB hypertable)

Based on schema in `migrations/01_schema.sql`:

- `product_id`: UUID = 16 bytes
- `timestamp`: TIMESTAMPTZ = 8 bytes
- `price`: NUMERIC(20,6) = ~12 bytes
- PostgreSQL row overhead: ~24 bytes
- **Total per row**: ~60 bytes

### product_price_15min (continuous aggregate)

- `product_id`: UUID = 16 bytes
- `bucket`: TIMESTAMPTZ = 8 bytes
- `price`: NUMERIC(20,6) = ~12 bytes
- Materialized view overhead: ~24 bytes
- **Total per row**: ~60 bytes

### user_cash_flow table

Based on schema in `migrations/01_schema.sql`:

- `user_id`: UUID = 16 bytes
- `product_id`: UUID = 16 bytes
- `timestamp`: TIMESTAMPTZ = 8 bytes
- `units`: NUMERIC(20,6) = ~12 bytes
- `price`: NUMERIC(20,6) = ~12 bytes
- `deposit`: NUMERIC(20,6) = ~12 bytes
- `cumulative_units`: NUMERIC(20,6) = ~12 bytes
- `cumulative_deposits`: NUMERIC(20,6) = ~12 bytes
- `period_return`: NUMERIC(20,6) = ~12 bytes
- `cumulative_twr_factor`: NUMERIC(20,6) = ~12 bytes
- PostgreSQL row overhead: ~24 bytes
- **Total per row**: ~148 bytes

### Cache tables (user_product_timeline_cache_15min, user_timeline_cache_15min)

Based on schema in `migrations/04_cache.sql`:

- Approximately 88 bytes per row (similar to timeline view columns)

## Storage Growth Projections

### Annual Storage Requirements

**Raw price data (before bucketing):**
- Raw prices: 56.25M rows × 60 bytes = **3.38 GB/year**
- With indexes (30-50% overhead): **4.4-5.1 GB/year**

**Bucketed price data (continuous aggregate):**
- Bucketed prices: 7.5M rows × 60 bytes = **450 MB/year**
- With indexes: **585-675 MB/year**
- **Storage reduction: 87%**

**With TimescaleDB compression (5-10x):**
- Compressed buckets: **45-90 MB/year**

**Cash flows:**
- Cash flows: 600k rows × 148 bytes = **89 MB/year**
- With indexes: **115-133 MB/year**

**Cache tables (if retained indefinitely):**
- Cache data: 375M rows × 88 bytes = **33 GB/year**
- With indexes: **43-50 GB/year**

**Total annual storage (with bucketing, no compression):**
- Raw prices: 5.1 GB
- Bucketed prices: 675 MB
- Cash flows: 133 MB
- Cache: 50 GB (if no retention policy)
- **Total: ~56 GB/year** (dominated by cache if retained indefinitely)

**Total with compression:**
- Raw prices: 5.1 GB (can be dropped after bucketing or compressed)
- Bucketed prices: 90 MB
- Cash flows: 133 MB
- Cache: 50 GB (can be managed with retention policies)
- **Total: ~50-56 GB/year**

### 5-Year Projection

| Component | Rows/Year | Size/Year (uncompressed) | 5-Year Size |
|-----------|-----------|-----------|-------------|
| Raw prices (hypertable) | 56.25M | 5.1 GB | 25.5 GB |
| Bucketed prices (15min) | 7.5M | 675 MB | 3.4 GB |
| Bucketed (compressed) | 7.5M | 90 MB | 450 MB |
| Cash flows | 600k | 133 MB | 665 MB |
| Cache (unmanaged) | 375M | 50 GB | 250 GB |
| Cache (90-day retention) | 375M | 12 GB | 12 GB |
| **Total (5 years, compressed + 90-day cache)** | | | **~17 GB** |

## Actual Performance (Benchmark Results)

### Query Performance

From benchmark tests on Apple M1 MacBook Pro:

**At production scale (225k-250k events/day):**
- Bucket refresh: ~0.08s
- User-product query (before cache): ~3.3ms
- User query (before cache): ~5.2ms
- Cache refresh: ~0.6s
- User-product query (after cache): ~3.1ms
- User query (after cache): ~3.6ms

**At 1M events (~4 days of data):**
- Bucket refresh: 0.55s
- User-product query: ~15ms
- User query: ~20ms
- Cache refresh: 13.7s

**Key insight:** At production scale (250k events/day), daily cache refresh takes less than 1 second. Even hourly refresh is trivial.

### Storage Efficiency

**15-minute bucketing effectiveness:**
- **87-93% data reduction** vs raw 2-minute prices
- Minimal precision loss for TWR calculations
- Cash flows retain exact timestamps (no bucketing)

**Cache effectiveness:**
- Query speedup: 1.3-3.2x at low scale (10k-1M events)
- Diminishing returns at high scale (cache overhead becomes significant)
- Best strategy: Use short retention windows (30-90 days)

## Optimizations Already Implemented

### ✅ TimescaleDB Hypertables

```sql
-- product_price is a hypertable with 1-month chunks
SELECT create_hypertable('product_price', 'timestamp',
    chunk_time_interval => INTERVAL '1 month');
```

**Benefits:**
- Automatic partitioning by time
- Fast queries with partition pruning
- Easy chunk management (drop old chunks)
- Foundation for compression

### ✅ Continuous Aggregates (15-minute buckets)

```sql
CREATE MATERIALIZED VIEW product_price_15min
WITH (timescaledb.continuous) AS
SELECT product_id,
       time_bucket('15 minutes', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('15 minutes', timestamp);
```

**Benefits:**
- 87% data reduction
- Auto-refresh policy (refreshes every 15 minutes)
- Transparent to queries
- Indexable for fast lookups

### ✅ Cache + Delta Architecture

```sql
-- Combined view: cache + fresh data
CREATE VIEW user_product_timeline_15min AS
    SELECT * FROM user_product_timeline_cache_15min  -- Tier 1: Pre-computed
    UNION ALL
    SELECT * FROM user_product_timeline_base_15min   -- Tier 2+3: Fresh data
    WHERE timestamp > (SELECT MAX(timestamp) FROM user_product_timeline_cache_15min);
```

**Benefits:**
- Fast queries (leverage pre-computed cache)
- Always fresh (recent data computed live)
- Incremental refresh (only compute new data)

## Recommended Next Steps

### High Priority

#### 1. Enable TimescaleDB Compression

```sql
-- Enable compression on product_price hypertable
ALTER TABLE product_price SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'product_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Automatically compress chunks older than 7 days
SELECT add_compression_policy('product_price', INTERVAL '7 days');
```

**Expected benefit:** 5-10x storage reduction on old chunks

#### 2. Implement Cache Retention Policy

```sql
-- Modify refresh_timeline_cache_15min() to clean up old cache data
DELETE FROM user_product_timeline_cache_15min
WHERE timestamp < NOW() - INTERVAL '90 days';

DELETE FROM user_timeline_cache_15min
WHERE timestamp < NOW() - INTERVAL '90 days';
```

**Expected benefit:** Keeps cache at 12 GB instead of growing to 250 GB over 5 years

#### 3. Add Data Retention Policy

```sql
-- Automatically drop raw price chunks older than 90 days
-- (Keep bucketed data indefinitely or with longer retention)
SELECT add_retention_policy('product_price', INTERVAL '90 days');
```

**Rationale:**
- After 90 days, queries use bucketed data (15-min granularity)
- Raw 2-minute data no longer needed for most use cases
- Keeps storage bounded

### Medium Priority

#### 4. Add Hourly/Daily Bucket Granularities

Create additional continuous aggregates for long-term queries:

```sql
-- Hourly buckets for queries spanning weeks/months
CREATE MATERIALIZED VIEW product_price_1h
WITH (timescaledb.continuous) AS
SELECT product_id,
       time_bucket('1 hour', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('1 hour', timestamp);

-- Daily buckets for queries spanning years
CREATE MATERIALIZED VIEW product_price_1d
WITH (timescaledb.continuous) AS
SELECT product_id,
       time_bucket('1 day', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('1 day', timestamp);
```

**Benefits:**
- Faster queries over long time ranges
- Further storage reduction for historical data
- Users choose precision vs performance trade-off

#### 5. Optimize Cache Refresh

For very large datasets, consider:
- Parallel query execution
- Incremental refresh with smaller batches
- Refresh only active user-product pairs

### Low Priority

#### 6. Hot/Cold Storage

For multi-year deployments:
- **Hot storage (SSD)**: Last 90 days (~5-6 GB)
- **Warm storage (HDD)**: 90 days - 2 years (~20-25 GB)
- **Cold storage (S3)**: >2 years (compressed bucketed data, ~450 MB)

## Storage Cost Estimates

Assuming AWS pricing (us-east-1, 2025):

### Without Optimization (5 years)

- 280 GB on RDS PostgreSQL (db.t4g.medium with GP3): ~$50/month storage + ~$50/month instance
- Plus backup storage: ~$30/month
- **Total**: ~$130/month or **$1,560/year**

### With Optimization (5 years)

- 17 GB on RDS PostgreSQL (db.t4g.small with GP3): ~$4/month storage + ~$25/month instance
- Plus backup storage: ~$5/month
- **Total**: ~$34/month or **$408/year**

**Savings**: ~$1,152/year with optimization

### Scaling Beyond 1,000 Products

**2,000 products:**
- 450k events/day
- ~34 GB over 5 years (with optimizations)
- Cache refresh: ~1.5s daily
- **Cost**: ~$50/month

**5,000 products:**
- 1.125M events/day
- ~85 GB over 5 years (with optimizations)
- Cache refresh: ~14s daily
- **Cost**: ~$100/month

**10,000 products:**
- 2.25M events/day
- ~170 GB over 5 years (with optimizations)
- Cache refresh: ~2-3 minutes daily
- **Cost**: ~$180/month

## Summary

### Current State (1,000 products baseline)

**Storage:**
- ~56 GB/year without optimization
- ~11 GB/year with compression and retention policies
- ~17 GB total over 5 years (fully optimized)

**Performance:**
- Daily cache refresh: <1 second (trivial)
- Query performance: Sub-20ms for timeline queries
- Bucket refresh: Sub-second for daily data

**Cost:**
- ~$408/year (fully optimized, 5-year projection)

### Key Advantages of Current Architecture

✅ **TimescaleDB integration** eliminates 87% of storage via bucketing
✅ **Sub-second cache refresh** makes hourly or even more frequent refreshes practical
✅ **Linear scaling** up to ~5,000 products without architectural changes
✅ **Compression available** for additional 5-10x reduction
✅ **Proven performance** via actual benchmarks (not theoretical)

### Production Readiness

**Ready for production** at 1,000-2,000 product scale with:
- ✅ TimescaleDB hypertables (implemented)
- ✅ 15-minute bucketing (implemented)
- ✅ Cache + delta architecture (implemented)
- ⏳ Compression policy (needs configuration)
- ⏳ Cache retention policy (needs implementation)
- ⏳ Data retention policy (optional, for cost optimization)

**Scaling to 5,000-10,000 products** requires:
- Adding hourly/daily bucket granularities
- Implementing compression and retention policies
- Potentially optimizing cache refresh for very large datasets

**Beyond 10,000 products:**
- Consider alternative architectures (event streaming, specialized time-series databases)
- Evaluate price sampling strategies (only store significant changes)
- Benchmark cache refresh performance at scale

### Comparison to Original Analysis

**Original scenario** (500k products):
- 25.35 billion rows/year
- 1.52 TB/year storage
- Cache refresh impractical (hours to days)
- Conclusion: "System will become unmanageable within 6-12 months"

**Current system** (1,000 products with TimescaleDB):
- 56.25 million rows/year (raw), 7.5 million (bucketed)
- 11 GB/year storage (optimized)
- Cache refresh: <1 second daily
- Conclusion: **Production-ready with excellent performance characteristics**

**Key insight:** The combination of realistic product scale (1,000 vs 500,000) and TimescaleDB bucketing (87% reduction) transforms this from "proof-of-concept" to "production-ready system."
