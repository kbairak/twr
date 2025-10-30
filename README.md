# Time-Weighted Return (TWR) Calculation System

A PostgreSQL/TimescaleDB-based system for calculating Time-Weighted Returns with smart bucketing and incremental caching.

## Why

**Time-Weighted Return (TWR)** measures investment performance independent of cash flow timing. It answers: *"How well did my investments perform, regardless of when I added or withdrew money?"*

This is different from simple ROI, which is affected by contribution timing. TWR isolates investment skill from timing decisions - which is why portfolio managers are evaluated using TWR.

### The Timeline Challenge

TWR calculation requires splitting the timeline at every cash flow event. But what about portfolio value *between* transactions?

**Example:** Alice buys 100 units of AAPL at $100. The price rises to $120 (no transaction). Then rises to $130, and she buys 50 more units.

To calculate TWR correctly, we need to know the portfolio state at **every price change**:

```
┃ timestamp ┃ event       ┃ holdings ┃ price   ┃  value    ┃ twr      ┃
│ t1        │ buy 100     │ 100.00   │ $100.00 │ $10,000   │   0.00%  │
│ t2        │ price ↑     │ 100.00   │ $120.00 │ $12,000   │  +20.00% │  ← no transaction!
│ t3        │ price ↑     │ 100.00   │ $130.00 │ $13,000   │  +30.00% │  ← no transaction!
│ t4        │ buy 50      │ 150.00   │ $130.00 │ $19,500   │  +30.00% │
```

Without tracking price changes at t2 and t3, we can't compute accurate TWR. Users may be inactive for months, but their portfolio value still changes with prices.

**This system generates timeline entries for every price change**, not just cash flows, enabling accurate TWR calculation at any point in time.

## Problem with Naive Approach

The obvious solution: for each price update, create timeline entries for every user holding that product.

**This scales catastrophically:**

- 10 products with 2-minute price updates during trading hours = 9,000 price updates/day per product
- 1,000 users holding each product = 9,000,000 timeline entries/day
- Over 1 year = **2.25 billion timeline entries**

With multiple products and users:

- **500 products × 50,000 users** = 25 million holdings
- **Each price update** = 25 million new timeline entries
- **Result**: System grinds to a halt within days

The naive approach creates **products × users × price_updates** timeline entries, which becomes unmanageable at scale.

## Buckets

Instead of creating timeline entries for every 2-minute price update, we **bucket prices into 15-minute intervals**.

### How It Works

**TimescaleDB continuous aggregates** automatically maintain 15-minute price buckets:

```sql
-- Continuous aggregate: last price in each 15-minute bucket
CREATE MATERIALIZED VIEW product_price_15min
WITH (timescaledb.continuous) AS
SELECT product_id,
       time_bucket('15 minutes', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('15 minutes', timestamp);
```

**Data reduction:**

- Raw 2-min intervals: ~9,000 prices/day per product (7.5 hours × 60 min / 2 min)
- 15-min buckets: ~1,200 prices/day per product (7.5 hours × 60 min / 15 min)
- **Reduction: 87-93%**

**Trade-off:**

- ✅ Massive performance improvement (87% fewer rows to process)
- ⚠️ Price precision reduced to 15-minute granularity
- ✅ Cash flows still use exact timestamps (precision where it matters)

### Auto-Refresh Policy

The continuous aggregate refreshes automatically every 15 minutes, keeping data fresh:

```sql
SELECT add_continuous_aggregate_policy('product_price_15min',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '15 minutes'
);
```

You can also refresh manually: `uv run main.py refresh-buckets`

### Extensibility

All views use the `_15min` suffix to support future granularities:

- `user_product_timeline_15min` (current)
- `user_product_timeline_1h` (future: hourly bucketing for longer time ranges)
- `user_product_timeline_1d` (future: daily bucketing for multi-year queries)

Users can choose precision vs. performance based on their needs.

## Cache

When you query timeline data, the system combines three sources to balance performance with freshness:

### Three-Tier Query Architecture

```
┌────────────────────────────────────────────────────────────────┐
│          Combined View (user_product_timeline_15min)           │
│                    What you actually query                      │
└────────────────────────────┬───────────────────────────────────┘
                             │ UNION ALL
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
    ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
    │   Tier 1    │  │    Tier 2    │  │    Tier 3    │
    │   CACHE     │  │   BUCKETS    │  │  RAW PRICES  │
    ├─────────────┤  ├──────────────┤  ├──────────────┤
    │ Historical  │  │ Recent data  │  │ Latest data  │
    │ pre-computed│  │ (15min       │  │ (exact       │
    │ timeline    │  │  bucketed)   │  │  timestamps) │
    │             │  │              │  │              │
    │ ≤ cache_max │  │ > cache_max  │  │ > last_bucket│
    │             │  │ ≤ last_bucket│  │              │
    └─────────────┘  └──────────────┘  └──────────────┘
         ▲                  ▲                  ▲
         │                  │                  │
         │                  +──────────────────┼─────────┐
         │                  │                  │         │
         │                  │                  │         │
    ┌────┴──────────┐  ┌──────────────────┐    │         │
    │  cache table  │  │ product prices   │    │         │
    │               │  │ (bucketed 15min) │    │         │
    │ refreshed via │  │ refreshed by     │    │         │
    │ refresh_cache │  │ timescaledb      │    │         │
    └───────────────┘  └──────────────────┘    │         │
                            ▲                  │         │
                            │            ┌─────┘         │
                            │            │               │
                       ┌──────────────────┐      ┌──────────────┐
                       │ product prices   │      │  cash_flow   │
                       │ (raw)            │      │  (raw)       │
                       └──────────────────┘      └──────────────┘
```

**Query Strategy:**

1. **Tier 1 (Cache)**: Pre-computed historical timeline from cache table
   - Fastest: Already computed and stored
   - Data source: `user_product_timeline_cache_15min`
   - Coverage: From first event up to last cache refresh

2. **Tier 2 (Bucketed Recent)**: Fresh timeline from 15-minute buckets
   - Fast: Uses bucketed prices (87% fewer rows)
   - Data source: `product_price_15min` continuous aggregate + `user_cash_flow`
   - Coverage: After cache up to last materialized bucket

3. **Tier 3 (Raw Fresh)**: Live timeline from raw price updates
   - Moderate: Uses exact price timestamps
   - Data source: `product_price` (raw) + `user_cash_flow`
   - Coverage: After last bucket up to current time

### How the Cache Works

The cache table shares the same schema as the base views. To populate it, we simply evaluate the base view and insert results:

```sql
-- Simplified refresh logic
INSERT INTO user_product_timeline_cache_15min
SELECT * FROM user_product_timeline_base_15min
WHERE timestamp > (SELECT MAX(timestamp) FROM user_product_timeline_cache_15min);
```

**Refresh command:** `uv run main.py refresh`

**Example timeline showing cache + delta:**

```
┃ timestamp  ┃ total_value ┃ twr_pct ┃ cached ┃ source
│ t1         │   $10,000   │   0.00% │   ✓    │  ← Tier 1: Cache
│ t2         │   $12,000   │  +20.00%│   ✓    │  ← Tier 1: Cache
│ t3         │   $18,000   │  +44.00%│   ✓    │  ← Tier 1: Cache
│ t4         │   $19,500   │  +30.00%│   ✗    │  ← Tier 2: 15min bucketed (after cache)
│ t5         │   $21,000   │  +40.00%│   ✗    │  ← Tier 3: Raw prices (after last bucket)
```

This architecture ensures fast queries (leverage cache) while maintaining freshness (compute recent data live).

## Value-Weighted TWR for Multi-Product Portfolios

When a user holds multiple products, we can't simply average their TWRs - a product with $100 invested shouldn't count the same as one with $10,000 invested.

The system uses **value-weighting** to compute portfolio-level TWR:

```sql
portfolio_twr = SUM(product_twr × product_value) / SUM(product_value)
```

**Example:** Alice holds two products:

- AAPL: $10,000 value, +30% TWR
- NVDA: $1,000 value, +80% TWR

Simple average: (30% + 80%) / 2 = **55%** ❌ (incorrect - treats them equally)

Value-weighted: (30% × $10,000 + 80% × $1,000) / $11,000 = **34.5%** ✓ (correct - AAPL dominates portfolio)

This ensures that larger positions appropriately influence the overall portfolio performance.

### Incremental TWR Calculation

Traditional TWR requires iterating all historical cash flows. This system stores the cumulative TWR factor at each cash flow using **database triggers**:

```sql
new_cumulative_twr_factor = previous_cumulative_twr_factor × (1 + period_return)
```

This enables **O(1) updates** on each cash flow insert. The trigger automatically calculates:

- `deposit` (money amount of the transaction)
- `cumulative_units` (total holdings after this flow)
- `cumulative_deposits` (total money invested after this flow)
- `period_return` (return since last cash flow)
- `cumulative_twr_factor` (compounded TWR factor up to this point)

## Quick Start

### Prerequisites

- Docker (for TimescaleDB)
- Python 3.13+
- uv (Python package manager)

### Interactive Demo

Let's walk through a complete example showing how buckets and cache work:

```bash
# 1. Start TimescaleDB
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Setup database
uv run main.py drop && uv run main.py migrate
```

Now let's simulate real trading activity:

```bash
# 4. Set initial price for AAPL
uv run main.py add-price --product AAPL --price 100.00
uv run main.py show
```

**Output after initial price:**

```
PRODUCT PRICES
┃ product ┃ price    ┃ timestamp
│ AAPL    │ $100.00  │ 2025-11-01 10:00:00
```

```bash
# 5. Alice buys $10,000 worth of AAPL
uv run main.py add-cashflow --user alice --product AAPL --money 10000
uv run main.py show
```

**Timeline after first buy:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
```

```bash
# 6. Price rises to $120
uv run main.py add-price --product AAPL --price 120.00
uv run main.py show
```

**Timeline after price increase:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │   ✗    │  ← price change created timeline entry!
```

```bash
# 7. Alice buys $6,000 more
uv run main.py add-cashflow --user alice --product AAPL --money 6000
uv run main.py show
```

**Timeline after second buy:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $120.00 │ $18000.00 │  20.00% │   ✗    │  ← cash flow TWR
```

```bash
# 8. Add more price updates
uv run main.py add-price --product AAPL --price 125.00
uv run main.py add-price --product AAPL --price 130.00
uv run main.py show
```

**Timeline grows with price changes:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $120.00 │ $18000.00 │  20.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $125.00 │ $18750.00 │  25.00% │   ✗    │
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $130.00 │ $19500.00 │  30.00% │   ✗    │  ← 5 timeline entries
```

Now let's see bucketing in action:

```bash
# 9. Refresh the 15-minute buckets (simulates continuous aggregate refresh)
uv run main.py refresh-buckets
uv run main.py show
```

**After bucket refresh:**
If the price updates happened within the same 15-minute window, they get merged into a single bucketed entry! The timeline will show fewer entries because prices that occurred in the same 15-minute bucket are collapsed to the last price in that bucket.

```bash
# 10. Refresh the cache (pre-compute timeline)
uv run main.py refresh
uv run main.py show
```

**Timeline showing cached data:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✓    │  ← cached
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │   ✓    │  ← cached
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $120.00 │ $18000.00 │  20.00% │   ✓    │  ← cached
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $130.00 │ $19500.00 │  30.00% │   ✓    │  ← cached
```

```bash
# 11. Add new price after cache (demonstrates delta)
uv run main.py add-price --product AAPL --price 140.00
uv run main.py show
```

**Timeline showing cache + delta:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✓    │  ← from cache
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │   ✓    │  ← from cache
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $120.00 │ $18000.00 │  20.00% │   ✓    │  ← from cache
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $130.00 │ $19500.00 │  30.00% │   ✓    │  ← from cache
│ alice │ AAPL    │ 2025-11-01 ...  │ 150.00   │    $16000.00 │ $140.00 │ $21000.00 │  40.00% │   ✗    │  ← computed live (delta)!
```

**Key observations:**

- Price changes automatically create timeline entries (no transaction needed)
- Bucketing reduces timeline entries when prices occur in same 15-min window
- Cache pre-computes historical data (✓)
- Recent data computed on-the-fly (✗)
- Cache + delta pattern keeps queries fast while staying fresh

## Sample SQL Commands

### Adding Price Updates

```sql
-- Insert a price update (product auto-created if doesn't exist)
INSERT INTO product (name) VALUES ('AAPL') ON CONFLICT DO NOTHING;

INSERT INTO product_price (product_id, price, timestamp)
SELECT id, 120.50, NOW()
FROM product WHERE name = 'AAPL';
```

### Adding Cash Flows

```sql
-- Insert a cash flow (trigger auto-populates computed fields)
-- Users and products are auto-created if they don't exist
INSERT INTO "user" (name) VALUES ('alice') ON CONFLICT DO NOTHING;
INSERT INTO product (name) VALUES ('AAPL') ON CONFLICT DO NOTHING;

INSERT INTO user_cash_flow (user_id, product_id, units, timestamp)
SELECT u.id, p.id, 100.0, NOW()
FROM "user" u, product p
WHERE u.name = 'alice' AND p.name = 'AAPL';
```

**Fields auto-populated by trigger:**

- `price`: Latest price at or before the cash flow timestamp
- `deposit`: Money amount (units × price)
- `cumulative_units`: Total holdings after this flow
- `cumulative_deposits`: Total money invested after this flow
- `period_return`: Return since last cash flow
- `cumulative_twr_factor`: Compounded TWR factor up to this point

**Required fields:** `user_id`, `product_id`, `units`, `timestamp`

**Auto-computed fields:** `price`, `deposit`, `cumulative_units`, `cumulative_deposits`, `period_return`, `cumulative_twr_factor`

### Querying Timeline Data

**Query user-product timeline for specific user and date range:**

```sql
SELECT
    timestamp,
    holdings,
    net_deposits,
    current_price,
    current_value,
    current_twr * 100 as twr_pct,
    is_cached
FROM user_product_timeline_15min
WHERE user_id = (SELECT id FROM "user" WHERE name = 'alice')
  AND product_id = (SELECT id FROM product WHERE name = 'AAPL')
  AND timestamp BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY timestamp;
```

**Query user timeline (aggregated across all products):**

```sql
SELECT
    timestamp,
    total_net_deposits,
    total_value,
    value_weighted_twr * 100 as twr_pct,
    is_cached
FROM user_timeline_15min
WHERE user_id = (SELECT id FROM "user" WHERE name = 'alice')
  AND timestamp BETWEEN '2025-01-01' AND '2025-12-31'
ORDER BY timestamp;
```

**Get latest overall portfolio state for a user:**

```sql
SELECT
    u.name as user,
    ut.timestamp,
    ut.total_net_deposits,
    ut.total_value,
    ut.value_weighted_twr * 100 as twr_pct
FROM user_timeline_15min ut
JOIN "user" u ON ut.user_id = u.id
WHERE ut.user_id = (SELECT id FROM "user" WHERE name = 'alice')
ORDER BY ut.timestamp DESC
LIMIT 1;
```

**Get latest state for each product a user holds:**

```sql
SELECT DISTINCT ON (upt.product_id)
    u.name as user,
    p.name as product,
    upt.holdings,
    upt.current_price,
    upt.current_value,
    upt.current_twr * 100 as twr_pct
FROM user_product_timeline_15min upt
JOIN "user" u ON upt.user_id = u.id
JOIN product p ON upt.product_id = p.id
WHERE upt.user_id = (SELECT id FROM "user" WHERE name = 'alice')
ORDER BY upt.product_id, upt.timestamp DESC;
```

## Benchmarking

### Event Generator

The event generator creates realistic synthetic data for testing and benchmarking.

**How it works:**

- **Realistic market timing**: Generates events during trading hours (9:30 AM - 4:00 PM)
- **Weekend handling**: Automatically skips Saturdays and Sundays
- **Price updates**: Every 2 minutes during market hours, synchronized across all products
  - Small jitter (milliseconds) to avoid exact timestamp collisions
  - Random walk: -2% to +2.5% per update (slightly bullish)
- **Cash flows**: Randomly distributed across time range
  - 80% during market hours, 20% after-hours
  - 90% price events, 10% cash flow events (9:1 ratio)
  - 80% buys, 20% sells
  - Users tend to invest in products they already own (90% probability)

**Event density (events per day of trading):**

With 2-minute price updates during 7.5-hour trading days:

- Price updates per product: 7.5 hours × 60 min / 2 min = **225 price updates/product/day**
- With 90/10 price/cashflow split: ~**250 total events/product/day**

**Real-world scale examples:**

- **500 products** = ~125,000 events/day
- **1,000 products** = ~250,000 events/day
- **2,000 products** = ~500,000 events/day

**Cache refresh implications:**
Based on benchmark results, daily cache refresh is highly practical:

- 125k events: Cache refresh ~0.2s (trivial for daily refresh)
- 250k events: Cache refresh ~0.8s (very practical for daily refresh)
- 500k events: Cache refresh ~2.5s (easily supports daily refresh)

Even with 1,000+ products generating 250k events/day, a daily cache refresh completes in under 1 second, making it practical to refresh every few hours or even hourly if needed.

**Sample usage:**

```bash
# Generate 100k events with 1000 users and 500 products
uv run python event_generator.py --num-events 100000 --num-users 1000 --num-products 500

# Large-scale test: 1M events
uv run python event_generator.py --num-events 1000000 --num-users 10000 --num-products 1000
```

**What it generates:**

- Product price updates following a random walk
- User cash flows with realistic buy/sell patterns
- Timeline spanning multiple days/months of trading activity

### Benchmark Script

The benchmark script measures system performance at various scales.

**What it measures:**

1. **Bucket refresh time**: How long to refresh the 15-minute continuous aggregate
2. **Query performance (before cache)**:
   - User-product timeline query (single user, single product)
   - User timeline query (single user, all products aggregated)
3. **Cache refresh time**: How long to populate the cache with historical data
4. **Query performance (after cache)**:
   - Same queries, but leveraging cached data

**Sample usage:**

```bash
# Small benchmark (quick test)
uv run python benchmark.py --num-events 10000 --num-users 100 --num-products 500

# Medium benchmark (realistic scale)
uv run python benchmark.py --num-events 100000 --num-users 1000 --num-products 1000

# Large benchmark (stress test)
uv run python benchmark.py --num-events 1000000 --num-users 10000 --num-products 1000
```

The script outputs detailed timing measurements and stores results in a SQLite database for analysis.

### Results

Performance on Apple M1 MacBook Pro with TimescaleDB 15-minute bucketing:

| Events | Users | Products | Bucket refresh | Avg user-product query (before cache) | Avg user query (before cache) | Cache refresh | Avg user-product query (after cache) | Avg user query (after cache) |
|--------|-------|----------|----------------|---------------------------------------|-------------------------------|---------------|--------------------------------------|------------------------------|
| 10k    | 100   | 1k       | 0.01s          | 2.31ms                                | 1.64ms                        | 0.01s         | 0.88ms                               | 1.74ms                       |
| 100k   | 1k    | 500      | 0.06s          | 2.04ms                                | 3.08ms                        | 0.30s         | 2.06ms                               | 1.96ms                       |
| 100k   | 1k    | 1k       | 0.05s          | 1.99ms                                | 3.07ms                        | 0.17s         | 1.90ms                               | 1.92ms                       |
| 200k   | 1k    | 500      | 0.09s          | 3.17ms                                | 4.99ms                        | 0.69s         | 2.95ms                               | 3.83ms                       |
| 200k   | 2k    | 1k       | 0.09s          | 3.40ms                                | 5.33ms                        | 0.58s         | 3.27ms                               | 3.32ms                       |
| 300k   | 1k    | 500      | 0.16s          | 4.25ms                                | 7.32ms                        | 1.55s         | 4.39ms                               | 4.31ms                       |
| 300k   | 3k    | 1k       | 0.17s          | 4.36ms                                | 8.27ms                        | 0.88s         | 4.60ms                               | 4.31ms                       |
| 400k   | 1k    | 500      | 0.22s          | 5.49ms                                | 9.75ms                        | 2.21s         | 5.36ms                               | 5.69ms                       |
| 400k   | 4k    | 1k       | 0.23s          | 5.81ms                                | 9.22ms                        | 1.59s         | 6.64ms                               | 4.38ms                       |
| 500k   | 1k    | 500      | 0.27s          | 10.00ms                               | 11.84ms                       | 2.95s         | 4.63ms                               | 4.91ms                       |
| 500k   | 3k    | 500      | 0.31s          | 7.30ms                                | 12.01ms                       | 4.05s         | 4.95ms                               | 5.39ms                       |
| 500k   | 3k    | 1k       | 0.29s          | 6.94ms                                | 11.37ms                       | 2.25s         | 5.15ms                               | 4.80ms                       |
| 600k   | 1k    | 500      | 0.38s          | 7.46ms                                | 12.16ms                       | 5.21s         | 5.64ms                               | 5.62ms                       |
| 600k   | 6k    | 1k       | 0.35s          | 8.46ms                                | 11.29ms                       | 4.45s         | 5.66ms                               | 5.96ms                       |
| 700k   | 1k    | 500      | 0.38s          | 16.97ms                               | 14.47ms                       | 6.99s         | 6.11ms                               | 6.50ms                       |
| 700k   | 7k    | 1k       | 0.42s          | 6.99ms                                | 13.17ms                       | 6.16s         | 7.01ms                               | 6.79ms                       |
| 800k   | 1k    | 500      | 0.43s          | 10.21ms                               | 19.04ms                       | 9.79s         | 6.75ms                               | 7.30ms                       |
| 800k   | 8k    | 1k       | 0.51s          | 11.25ms                               | 17.99ms                       | 8.79s         | 8.44ms                               | 7.78ms                       |
| 900k   | 1k    | 500      | 0.49s          | 11.45ms                               | 23.95ms                       | 12.39s        | 7.56ms                               | 7.75ms                       |
| 900k   | 9k    | 1k       | 0.47s          | 11.92ms                               | 21.03ms                       | 9.84s         | 8.33ms                               | 8.89ms                       |
| 1M     | 1k    | 500      | 0.54s          | 11.63ms                               | 26.72ms                       | 15.06s        | 8.39ms                               | 8.36ms                       |
| 1M     | 10k   | 500      | 0.56s          | 14.85ms                               | 20.87ms                       | 28.89s        | 10.15ms                              | 9.73ms                       |
| 1M     | 10k   | 1k       | 0.55s          | 15.80ms                               | 19.78ms                       | 13.71s        | 9.94ms                               | 9.94ms                       |
| 2M     | 20k   | 500      | 1.96s          | 32.76ms                               | 33.25ms                       | 3m 4.7s       | 26.29ms                              | 33.06ms                      |
| 2M     | 20k   | 1k       | 2.08s          | 19.42ms                               | 30.71ms                       | 1m 49.7s      | 24.99ms                              | 21.14ms                      |
| 5M     | 50k   | 500      | 11.33s         | 53.54ms                               | 70.18ms                       | 13m 5.7s      | 46.49ms                              | 43.51ms                      |
| 5M     | 50k   | 1k       | 8.98s          | 52.68ms                               | 70.25ms                       | 16m 40.4s     | 63.97ms                              | 97.21ms                      |

### Insights

**Bucketing effectiveness:**

- **87-93% data reduction**: 15-minute bucketing reduces timeline entries from ~9,000/day to ~1,200/day per product
- **Bucket refresh scales well**: Sub-second up to 1M events, ~11s at 5M events
  - 10k: 0.01s → 1M: 0.56s → 5M: 11.33s (roughly linear scaling)

**Query performance with bucketing:**

- **Up to 1M events**: Sub-20ms queries (2-16ms user-product, 2-27ms user aggregation)
- **At 2M events**: 20-33ms queries (still very responsive)
- **At 5M events**: 43-97ms queries (acceptable for most applications)

**Cache effectiveness:**

- **Low scale (10k-1M)**: 1.3-3.2x speedup, most effective for user aggregation queries
- **Medium scale (2M)**: 1.25-1.45x speedup, diminishing returns
- **High scale (5M)**: Mixed results (0.72-1.61x), cache overhead becomes significant

**Cache refresh scaling:**

- **Superlinear growth**: 1M: 13-29s → 2M: 1.8-3.1m → 5M: 13-17m
- **Product count matters**: More products = faster refresh per event (better data density)
  - 2M/20k/1k: 1.8m vs 2M/20k/500: 3.1m

**Production readiness:**

- **Sweet spot**: Sub-50ms cached queries up to 2M events
- **Acceptable**: 50-100ms queries at 5M events
- **Bottleneck**: Cache refresh becomes impractical beyond 5M events without optimization

**Scaling limits:**

- **10M events**: Hit numeric overflow with `NUMERIC(20,6)` precision (18,000+ compounding periods)
- **Solution**: Increase precision or use logarithmic TWR calculation for extreme scales

## Storage & Production Scale

This system uses **TimescaleDB with 15-minute bucketing** to handle production scale efficiently. See [STORAGE_ANALYSIS.md](STORAGE_ANALYSIS.md) for detailed analysis.

**Production baseline (1,000 products, 2-min price updates):**

- **Raw data**: 225 price updates/product/day × 1,000 products = 225,000 events/day
- **Annual raw storage**: ~82M price records/year ≈ 3.3 GB/year (without compression)
- **With 15-min bucketing**: 87% reduction → ~10.7M bucketed prices/year ≈ 430 MB/year
- **With TimescaleDB compression**: Additional 5-10x reduction → ~43-86 MB/year for bucketed data

**Performance at production scale:**

- **Daily cache refresh**: 225k events = ~0.8s (trivial, can refresh hourly)
- **Query performance**: Sub-20ms for user timelines (up to 1M events in cache)
- **Storage growth**: Approximately 430 MB/year of bucketed price data (manageable for years)

**Key optimizations already implemented:**

- ✅ TimescaleDB hypertables with 1-month chunks
- ✅ 15-minute continuous aggregates (87% data reduction)
- ✅ Auto-refresh policy for continuous aggregates
- ✅ Cache + delta architecture for fast queries

**Still needed for multi-year production:**

- [ ] Enable TimescaleDB compression on old chunks (5-10x additional reduction)
- [ ] Data retention policies (e.g., keep raw prices for 90 days, bucketed data for 7 years)
- [ ] Optional: Add hourly/daily bucket granularities for long-term historical queries

**Scalability:** With current architecture, the system can comfortably handle 1,000-2,000 products with years of historical data before requiring additional optimization.

## Database Schema

### Tables

- **`product`**: id (UUID), name
- **`product_price`**: product_id, timestamp, price — **TimescaleDB hypertable** (1-month chunks)
- **`user`**: id (UUID), name
- **`user_cash_flow`**: user_id, product_id, timestamp, units, price, deposit, cumulative_units, cumulative_deposits, period_return, cumulative_twr_factor
- **`user_product_timeline_cache_15min`**: Cached timeline per user-product
- **`user_timeline_cache_15min`**: Cached aggregated timeline per user

### Views & Continuous Aggregates

- **`product_price_15min`**: **TimescaleDB continuous aggregate** with 15-minute buckets (auto-refreshes every 15 min)
- **`user_product_timeline_base_15min`**: Computes portfolio state at each event (expensive, used for cache population)
- **`user_product_timeline_15min`**: **Combined cache + delta** (fast, what you should query)
- **`user_timeline_base_15min`**: Aggregates across products per user
- **`user_timeline_15min`**: **Combined cache + delta** for user-level data

### Functions

- **`refresh_timeline_cache_15min()`**: Incrementally refresh cache tables with new data

**Note:** Database constraints removed for maximum insertion performance (~2x speedup). Data consistency enforced at application level.

## Project Structure

```
/twr
   main.py                          # CLI interface
   event_generator.py               # Synthetic data generation
   benchmark.py                     # Performance benchmarking
   migrations/
      01_schema.sql                 # Foundation: TimescaleDB, tables, hypertables
      02_triggers.sql               # Business logic: TWR calculation
      03_base_views.sql             # Query infrastructure: bucketed + base views
      04_cache.sql                  # Performance layer: cache tables and refresh function
      05_combined_views.sql         # User-facing views: cache + delta pattern
   tests/
      test_twr.py                   # Test suite
   README.md
   STORAGE_ANALYSIS.md              # Storage projections and optimization
   pyproject.toml
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v
```

Tests cover database isolation, TWR calculation correctness, incremental updates, and cache functionality using isolated PostgreSQL containers.

## TWR vs Simple ROI

**TWR measures investment skill** - how well the products/assets themselves performed - independent of your timing decisions about when/how much to invest.

- **TWR answers:** "How good were my picks?"
- **Simple ROI/MWR answers:** "How much money did I make?"

### Formulas

**Simple ROI** doesn't account for timing of cash flows:

```
ROI = (Current Value - Total Invested) / Total Invested
```

**TWR** eliminates the impact of cash flow timing:

```
TWR = [(1 + r_period1) × (1 + r_period2) × ...] - 1
```

### Example: Why Timing Doesn't Affect TWR

**Scenario 1:** You invest $10,000 in AAPL at $100. It rises to $150 (+50%). You invest another $100,000 at $150. It drops to $140.

- **TWR:** ~-7% (the investment itself lost value from $150 → $140)
- **Simple ROI:** ~+21% (because most of your money was invested near the top)

**Scenario 2:** Same price movements, but you invest $100,000 at $100, then $10,000 at $150.

- **TWR:** Still ~-7% (same investment performance)
- **Simple ROI:** ~+27% (because most money was invested early)

**Key insight:** TWR is identical in both scenarios because the underlying investment performed the same way. The timing of your contributions doesn't matter - only the asset's performance matters.

This is why portfolio managers are judged by TWR - it isolates their stock-picking ability from the client's contribution timing. TWR tells you: "Ignoring when I added/withdrew money, were my product choices wise?"

## Next Steps

### High Priority

- [ ] **Compression policy for TimescaleDB hypertable**: Add compression to `product_price` hypertable to reduce storage costs (discuss compression strategy: chunk size, segment-by columns, order-by columns)
- [ ] **Add 1h and 1d bucket granularities**: Extend bucketing architecture to support hourly and daily views for longer time ranges
- [ ] **Update STORAGE_ANALYSIS.md**: Reflect that TimescaleDB is now implemented, update projections with actual benchmark data

### Future Enhancements

- [ ] Add period return column to timeline display (show price change % between events)
- [ ] Decouple money and units for cash flows (provider fees)
- [ ] Table partitioning for price table (in addition to TimescaleDB chunks)
- [ ] Data retention policies (automatic cleanup of old data)
- [ ] Bulk insert optimization using COPY protocol
- [ ] Money-Weighted Return (MWR/IRR) calculation
- [ ] Web dashboard for visualizing TWR over time
- [ ] Support for dividends and corporate actions
- [ ] Multi-currency support with FX conversion
- [ ] Parallel query execution for cache refresh
- [ ] Logarithmic TWR calculation to avoid numeric overflow at extreme scales

## References

- [Time-Weighted Return Explanation](https://www.investopedia.com/terms/t/time-weightedror.asp)
- [PostgreSQL Triggers](https://www.postgresql.org/docs/current/triggers.html)
- [TimescaleDB Continuous Aggregates](https://docs.timescale.com/use-timescale/latest/continuous-aggregates/)
- [TimescaleDB Hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/)
