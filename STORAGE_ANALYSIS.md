# Database Storage Growth Analysis

## Scenario Parameters

- **Products**: 500,000
- **Users**: 30,000
- **Price updates**: Every 2 minutes during market hours
- **Cash flows**: 5 per user per month (average)
- **Market hours**: 6.5 hours/day, 5 days/week (US market standard)

## Annual Data Growth Calculations

### Price Updates (product_price table)

- Market hours per day: 6.5 hours = 390 minutes
- Updates per product per day: 390 / 2 = 195 updates
- Updates per product per year: 195 × 5 days/week × 52 weeks = 50,700 updates
- **Total price rows per year**: 500,000 products × 50,700 = **25.35 billion rows/year**

### Cash Flows (user_cash_flow table)

- Cash flows per user per year: 5 × 12 = 60
- **Total cash flow rows per year**: 30,000 users × 60 = **1.8 million rows/year**

## Row Size Estimates

### product_price table
Based on schema in `migrations/01_create_tables.sql:8-19`:

- `product_id`: UUID = 16 bytes
- `timestamp`: TIMESTAMPTZ = 8 bytes
- `price`: NUMERIC(20,6) = ~12 bytes
- PostgreSQL row overhead: ~24 bytes
- **Total per row**: ~60 bytes

### user_cash_flow table
Based on schema in `migrations/01_create_tables.sql:27-58`:

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

## Storage Growth Projections

### Annual Storage Requirements

**Raw Data:**
- Price data: 25.35B rows × 60 bytes = **1.52 TB/year**
- Cash flow data: 1.8M rows × 148 bytes = **266 MB/year**
- **Total raw data**: ~**1.52 TB/year**

**With Indexes:**
Based on indexes in `migrations/01_create_tables.sql:17-18, 57-58`:
- B-tree indexes typically add 30-50% overhead
- **Total with indexes**: ~**2-2.3 TB/year**

### Cache Tables Growth

Based on `migrations/04_create_cache.sql:14-35` and watermark table:

**Realistic scenario** (users hold ~10 products on average):
- Active user-product pairs: 30,000 × 10 = 300,000 pairs
- Events per pair per year: ~50,700 price updates
- Total cache rows per year: 300,000 × 50,700 = 15.21 million rows
- Cache row size: ~88 bytes (user_product_timeline_cache)
- **Cache growth**: ~**1.34 GB/year** (without retention policy)
- Watermark table: negligible (single row)

**Worst case** (all users trade all products):
- User-product combinations: 30,000 × 500,000 = 15 billion combinations
- This would be impractical and require terabytes just for cache

## 5-Year Projection

| Component | Rows/Year | Size/Year | 5-Year Size |
|-----------|-----------|-----------|-------------|
| product_price (raw) | 25.35B | 1.52 TB | 7.6 TB |
| product_price (indexed) | 25.35B | 2.3 TB | 11.5 TB |
| user_cash_flow | 1.8M | 266 MB | 1.3 GB |
| cache (unmanaged) | 15M | 1.34 GB | 6.7 GB |
| **Total (5 years)** | **~127B rows** | | **~12 TB** |

## Key Issues

### 1. Price Table Dominates Storage (99.9% of data)

At 1.52 TB/year just for prices, storage becomes expensive quickly. The price table will contain 25 billion new rows every year.

### 2. Cache Strategy Accumulates Indefinitely

Currently, the cache system (from `migrations/04_create_cache.sql`) stores ALL historical events without any retention policy. This means:
- Cache grows at ~1.34 GB/year per 300k active user-product pairs
- No automatic cleanup mechanism
- Cache refresh (`refresh_timeline_cache()`) adds data but never removes old data
- Watermark-based incremental refresh helps performance but doesn't limit storage

### 3. Query Performance Degradation

The `user_product_timeline_base` view (from `migrations/03_create_base_views.sql`) performs:
- Correlated subqueries on billions of rows
- Lateral joins across user-product pairs
- Multiple ORDER BY ... LIMIT 1 operations per row

This will become progressively slower as data grows.

**Cache refresh performance** (from recent benchmarks on Apple M1):
- 100k events: 1.9s
- 500k events: 59s
- 900k events: 5m 22s

At production scale (25.35B events/year), cache refresh would take days without partitioning and would likely be impractical.

### 4. Index Growth

Indexes defined in `migrations/01_create_tables.sql`:
- `idx_product_prices_product_id_timestamp`: Will grow to ~700 GB over 5 years
- `idx_user_cash_flows_user_product`: Relatively small (~400 MB over 5 years)

## Recommended Optimizations

### 1. Table Partitioning

Partition `product_price` by time range (monthly or quarterly):

```sql
CREATE TABLE product_price (
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    price NUMERIC(20, 6) NOT NULL CHECK (price > 0)
) PARTITION BY RANGE (timestamp);

-- Create monthly partitions
CREATE TABLE product_price_2025_01 PARTITION OF product_price
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
```

Benefits:
- Fast partition dropping for old data
- Improved query performance (partition pruning)
- Easier backup/restore strategies

### 2. Data Archival Strategy

Implement time-based archival:
- Keep last 1-2 years in hot storage (frequently accessed)
- Move older data to cold storage or compressed partitions
- Drop partitions older than 5 years (or per retention policy)

### 3. Cache Retention Policy

Modify `refresh_timeline_cache()` function in `migrations/04_create_cache.sql` to only retain recent data:

```sql
-- Add retention to cache refresh function
DELETE FROM user_product_timeline_cache
WHERE timestamp < now() - interval '90 days';

DELETE FROM user_timeline_cache
WHERE timestamp < now() - interval '90 days';
```

Or use partial indexes:

```sql
CREATE INDEX ON user_product_timeline_cache (user_id, product_id, timestamp)
WHERE timestamp > now() - interval '90 days';
```

### 4. Price Sampling/Deduplication

Instead of storing every 2-minute tick, consider:

**Option A: Only store significant changes**
```sql
-- Only insert if price changed by >0.1%
INSERT INTO product_price (product_id, timestamp, price)
SELECT ...
WHERE abs(new_price - old_price) / old_price > 0.001;
```

**Option B: Hourly/daily snapshots**
```sql
-- Store end-of-hour prices instead of every 2 minutes
-- Reduces storage by 30x
```

### 5. Compression

Enable PostgreSQL table compression:

```sql
ALTER TABLE product_price SET (toast_compression = lz4);
-- Or for older PostgreSQL versions
ALTER TABLE product_price SET (compression = pglz);
```

Expected compression ratio: 3-5x for time-series data

### 6. Consider TimescaleDB

For time-series workloads, TimescaleDB extension provides:
- Automatic partitioning (hypertables)
- Built-in compression (10-20x)
- Continuous aggregates (pre-computed rollups with auto-refresh)
- Data retention policies

```sql
CREATE EXTENSION timescaledb;

SELECT create_hypertable('product_price', 'timestamp');
SELECT add_compression_policy('product_price', INTERVAL '7 days');
SELECT add_retention_policy('product_price', INTERVAL '5 years');
```

**Important Limitation**: TimescaleDB continuous aggregates can **only** be created directly on hypertables or other continuous aggregates. They **cannot** be created on top of regular PostgreSQL views, CTEs, or subqueries. This is a fundamental architectural constraint.

**Impact on current architecture**: The current `user_product_timeline_base` view (from `migrations/04_create_views.sql`) uses complex CTEs with lateral joins and correlated subqueries. To use TimescaleDB continuous aggregates, you would need to:

1. Convert `product_price` and `user_cash_flow` to hypertables
2. Restructure queries to work directly on hypertables (no intermediate views)
3. Potentially denormalize or pre-compute some fields to avoid complex joins

**Alternative approach**: Keep the current architecture and use traditional materialized views with manual refresh scheduling, or implement the cache system with retention policies as currently designed.

### 7. Hot/Cold Storage Architecture

Separate frequently accessed data from historical:
- **Hot storage** (SSD): Last 30-90 days (~200-600 GB)
- **Warm storage** (HDD): 90 days - 2 years (~3-5 TB)
- **Cold storage** (S3/Glacier): >2 years (archived, compressed)

## Storage Cost Estimates

Assuming AWS pricing (us-east-1, 2025):

**Without Optimization (5 years):**
- 12 TB on RDS PostgreSQL (db.r6g.2xlarge with GP3): ~$1,440/month
- Plus backup storage: ~$300/month
- **Total**: ~$1,740/month or **$20,880/year**

**With Optimization (5 years):**
- 2 TB hot storage (RDS): ~$240/month
- 4 TB warm storage (EBS): ~$400/month
- 6 TB cold storage (S3 Glacier): ~$24/month
- **Total**: ~$664/month or **$7,968/year**

**Savings**: ~$13,000/year with optimization

## Summary

**Current trajectory**: ~2-3 TB/year growth, reaching ~12 TB over 5 years

**Bottleneck**: Price table accounts for 99.9% of storage

**Critical needs**:
1. Table partitioning (immediate priority)
2. Cache retention policy (prevents unbounded growth)
3. Data archival strategy (reduces costs by 60%+)
4. Consider TimescaleDB for time-series optimization

**Without optimization**: Storage costs will grow linearly and query performance will degrade significantly after year 2-3. Cache refresh becomes impractical (hours to days) beyond ~1M events.

**With optimization**: Can reduce storage to ~2-4 TB over 5 years with better performance and 60% cost savings. However, production use at the described scale (500k products, 2-minute updates) remains challenging and requires careful infrastructure planning.

## Production Viability Warning

**This is a proof-of-concept system.** The scenario parameters (500k products with 2-minute price updates) represent an extremely demanding workload:

- **25.35 billion rows/year** is comparable to high-frequency trading systems
- Even with optimizations, managing this scale requires enterprise-grade infrastructure
- Alternative approaches to consider for production:
  - Reduce price update frequency (hourly instead of 2-minute)
  - Implement price sampling (only store significant changes)
  - Use specialized time-series databases (TimescaleDB, InfluxDB)
  - Consider event streaming architectures (Kafka + real-time aggregation)

**Realistic production scale**: System has been tested up to 900k events (5m22s cache refresh). Without partitioning and retention policies, the system will become unmanageable within 6-12 months at production scale.
