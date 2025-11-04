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

## Multi-Granularity Bucketing

Instead of creating timeline entries for every 2-minute price update, the system **buckets prices at multiple granularities** to balance precision, performance, and storage for different use cases.

### Three Granularities

The system provides three pre-configured granularities, each optimized for different time ranges:

| Granularity | Bucket Size | Real-time Data | Cache Retention | Best For |
|-------------|-------------|----------------|-----------------|----------|
| **15min**   | 15 minutes  | ✓ Yes          | 7 days          | Real-time monitoring, recent detailed analysis |
| **1h**      | 1 hour      | ✗ No           | 30 days         | Weekly/monthly performance analysis |
| **1d**      | 1 day       | ✗ No           | Indefinite      | Long-term trends, multi-year analysis |

**How it works:**

- Each granularity has its own **TimescaleDB continuous aggregate** for bucketed prices
- Each granularity has its own **cache tables** with automatic retention policies
- Each granularity has its own **combined views** (cache + delta pattern)

### 15min Granularity: Real-Time + Historical

The 15-minute granularity uses a **three-tier query architecture** for maximum freshness:

```sql
CREATE MATERIALIZED VIEW product_price_15min AS
SELECT product_id,
       time_bucket('15 minutes', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('15 minutes', timestamp);
```

**Data reduction:**

- Raw 2-min intervals: ~9,000 prices/day per product
- 15-min buckets: ~1,200 prices/day per product
- **Reduction: 87-93%**

**Three-tier query:**

1. **Tier 1 (Cache)**: Historical data from cache table (fast)
2. **Tier 2 (Bucketed)**: Recent data from 15-min buckets (fast)
3. **Tier 3 (Raw)**: Latest raw prices after last bucket (accurate)

**Cache retention:** 7 days (balances storage with recency needs)

### 1h Granularity: Medium-Term Analysis

Hourly bucketing provides efficient querying for weekly and monthly timelines:

```sql
CREATE MATERIALIZED VIEW product_price_1h AS
SELECT product_id,
       time_bucket('1 hour', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('1 hour', timestamp);
```

**Data reduction:**

- 15-min buckets: ~1,200 prices/day per product
- 1-hour buckets: ~300 prices/day per product
- **Reduction: 75% vs 15min, 97% vs raw**

**Two-tier query** (no real-time tier):

1. **Tier 1 (Cache)**: Historical data from cache table
2. **Tier 2 (Bucketed)**: Recent data from 1-hour buckets

**Cache retention:** 30 days (holds a month of detailed data)

### 1d Granularity: Long-Term Trends

Daily bucketing enables efficient multi-year historical queries:

```sql
CREATE MATERIALIZED VIEW product_price_1d AS
SELECT product_id,
       time_bucket('1 day', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('1 day', timestamp);
```

**Data reduction:**

- 1-hour buckets: ~300 prices/day per product
- 1-day buckets: ~4 prices/day per product (market open to close)
- **Reduction: 98.6% vs 15min, 99.96% vs raw**

**Two-tier query** (no real-time tier):

1. **Tier 1 (Cache)**: Historical data (kept indefinitely)
2. **Tier 2 (Bucketed)**: Recent data from daily buckets

**Cache retention:** Indefinite (daily granularity is compact enough to keep forever)

### Auto-Refresh Policies

Each continuous aggregate refreshes automatically at its bucket interval:

```sql
-- Refreshes every 15 minutes, 1 hour, or 1 day respectively
SELECT add_continuous_aggregate_policy('product_price_15min',
    schedule_interval => INTERVAL '15 minutes');
SELECT add_continuous_aggregate_policy('product_price_1h',
    schedule_interval => INTERVAL '1 hour');
SELECT add_continuous_aggregate_policy('product_price_1d',
    schedule_interval => INTERVAL '1 day');
```

**Manual refresh:**

```bash
# Refresh all granularities (default)
uv run main.py db refresh-buckets

# Refresh specific granularity
uv run main.py db refresh-buckets --granularity 15min
uv run main.py db refresh-buckets --granularity 1h
uv run main.py db refresh-buckets --granularity 1d
```

### Cache Retention Policies

Each granularity automatically deletes old cache entries based on its retention policy:

- **15min**: Deletes cache entries older than 7 days (keeps only recent detailed data)
- **1h**: Deletes cache entries older than 30 days (keeps a month of data)
- **1d**: Never deletes (indefinite retention for daily summaries)

**Manual cache refresh:**

```bash
# Refresh cache for all granularities (default)
uv run main.py db refresh

# Refresh cache for specific granularity
uv run main.py db refresh --granularity 15min
uv run main.py db refresh --granularity 1h
uv run main.py db refresh --granularity 1d
```

### Choosing a Granularity

**Use 15min when:**

- Analyzing today's or this week's performance
- Need real-time accuracy (includes raw prices after last bucket)
- Monitoring active trading periods

**Use 1h when:**

- Analyzing last week or last month
- Don't need minute-by-minute precision
- Want faster queries than 15min

**Use 1d when:**

- Analyzing trends over months or years
- Generating charts for multi-year performance
- Want maximum query speed and minimal storage

## Cache

When you query timeline data, the system combines three sources to balance performance with freshness:

### Three-Tier Query Architecture

```
┌────────────────────────────────────────────────────────────────┐
│          Combined View (user_product_timeline_15min)           │
│                    What you actually query                     │
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
uv run main.py db reset
```

Now let's simulate real trading activity:

```bash
# 4. Set initial price for AAPL
uv run main.py add price --product AAPL --price 100.00
uv run main.py query all
```

**Output after initial price:**

```
PRODUCT PRICES
┃ product ┃ price    ┃ timestamp
│ AAPL    │ $100.00  │ 2025-11-01 10:00:00
```

```bash
# 5. Alice buys $10,000 worth of AAPL
uv run main.py add cashflow --user alice --product AAPL --money 10000
uv run main.py query all
```

**Timeline after first buy:**

```
USER-PRODUCT TIMELINE
┃ user  ┃ product ┃ timestamp       ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ alice │ AAPL    │ 2025-11-01 ...  │ 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
```

```bash
# 6. Price rises to $120
uv run main.py add price --product AAPL --price 120.00
uv run main.py query all
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
uv run main.py add cashflow --user alice --product AAPL --money 6000
uv run main.py query all
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
uv run main.py add price --product AAPL --price 125.00
uv run main.py add price --product AAPL --price 130.00
uv run main.py query all
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
uv run main.py db refresh-buckets
uv run main.py query all
```

**After bucket refresh:**
If the price updates happened within the same 15-minute window, they get merged into a single bucketed entry! The timeline will show fewer entries because prices that occurred in the same 15-minute bucket are collapsed to the last price in that bucket.

```bash
# 10. Refresh the cache (pre-compute timeline)
uv run main.py db refresh
uv run main.py query all
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
uv run main.py add price --product AAPL --price 140.00
uv run main.py query all
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

The event generator creates realistic synthetic data for testing and benchmarking using a **2-of-3 parameter model**.

**Flexible parameter model:**

Provide **any 2 of these 3 parameters**, and the third is calculated automatically:

1. **--days**: Number of trading days to simulate
2. **--num-events**: Total number of events to generate
3. **--price-update-frequency**: How often prices update (e.g., "2min", "5min", "1h")

**Examples:**

```bash
# Specify days + frequency → calculates num-events
uv run main.py generate --days 10 --price-update-frequency 2min

# Specify days + num-events → calculates frequency
uv run main.py generate --days 5 --num-events 100000

# Specify num-events + frequency → calculates days
uv run main.py generate --num-events 50000 --price-update-frequency 5min
```

**How it works:**

- **Realistic market timing**: Generates events during trading hours (9:30 AM - 4:00 PM)
- **Weekend handling**: Automatically skips Saturdays and Sundays
- **Price updates**: Synchronized across all products at specified frequency
  - Small jitter (milliseconds) to avoid exact timestamp collisions
  - Random walk: -2% to +2.5% per update (slightly bullish)
- **Cash flows**: Randomly distributed across time range
  - 80% during market hours, 20% after-hours
  - 90% price events, 10% cash flow events (9:1 ratio)
  - 80% buys, 20% sells
  - Users tend to invest in products they already own (90% probability)

**Event density (events per day of trading):**

With 2-minute price updates during 6.5-hour trading days:

- Price updates per product: 6.5 hours × 60 min / 2 min = **195 price updates/product/day**
- With 90/10 price/cashflow split: ~**217 total events/product/day**

**Real-world scale examples:**

- **500 products** = ~108,500 events/day
- **1,000 products** = ~217,000 events/day
- **2,000 products** = ~434,000 events/day

**Cache refresh implications:**
Based on benchmark results, daily cache refresh is highly practical:

- 108k events: Cache refresh ~0.2s (trivial for daily refresh)
- 217k events: Cache refresh ~0.7s (very practical for daily refresh)
- 434k events: Cache refresh ~2.2s (easily supports daily refresh)

Even with 1,000+ products generating 217k events/day, a daily cache refresh completes in under 1 second, making it practical to refresh every few hours or even hourly if needed.

**What it generates:**

- Product price updates following a random walk
- User cash flows with realistic buy/sell patterns
- Timeline spanning multiple days/months of trading activity
- Automatically refreshes continuous aggregates for all granularities after generation

### Benchmark Script

The benchmark script measures system performance at various scales using the **2-of-3 parameter model**.

**What it measures:**

1. **Bucket refresh time**: How long to refresh continuous aggregates (all granularities)
2. **Query performance (before cache)**:
   - User-product timeline query (single user, single product)
   - User timeline query (single user, all products aggregated)
3. **Cache refresh time**: How long to populate the cache with historical data (for selected granularity)
4. **Query performance (after cache)**:
   - Same queries, but leveraging cached data

**Flexible parameter model:**

Provide **any 2 of these 3 parameters**, and the third is calculated automatically:

1. **--days**: Number of trading days to simulate
2. **--num-events**: Total number of events to generate
3. **--price-update-frequency**: How often prices update (e.g., "2min", "5min", "1h")

**Sample usage:**

```bash
# Benchmark 10 days of data with 2min updates
uv run main.py benchmark --days 10 --price-update-frequency 2min

# Benchmark 100k events over 5 trading days
uv run main.py benchmark --days 5 --num-events 100000

# Benchmark 50k events with 5min price updates
uv run main.py benchmark --num-events 50000 --price-update-frequency 5min
```

**Additional parameters:**

- **--num-users**: Number of users (default: 50)
- **--num-products**: Number of products (default: 100)
- **--num-queries**: Number of queries to sample (default: 100)

The script outputs detailed timing measurements showing:

- Event generation and insertion time
- Bucket refresh time for all granularities
- Query performance before and after cache refresh
- Cache refresh time for the selected granularity
- Speedup comparison (before vs after cache)

### Results

Performance benchmarks on Apple M1 MacBook Pro with TimescaleDB multi-granularity system (15min, 1h, 1d).

Each section explores one parameter while keeping others stable to isolate its impact on performance.

#### Section 1: Varying Trading Days

**Fixed parameters**: 2min price update frequency, 100 users, 500 products

| Days | Events | Bucket Refresh | Cache Refresh | Queries 15min (UP/U) | Speedup | Queries 1h (UP/U) | Speedup | Queries 1d (UP/U) | Speedup |
|------|--------|----------------|---------------|----------------------|---------|-------------------|---------|-------------------|---------|
| 5    | 542k   | 1.57s          | 12.28s/3.42s/1.32s | 5.00ms/31.24ms → 4.92ms/6.72ms | 3.26x | 4.68ms/9.12ms → 3.99ms/5.47ms | 1.37x | 3.89ms/6.33ms → 3.88ms/5.66ms | 1.08x |
| 10   | 1.08M  | 1.75s          | 26.16s/11.84s/3.26s | 15.49ms/84.51ms → 10.00ms/10.86ms | 4.70x | 8.13ms/24.03ms → 9.02ms/12.07ms | 1.52x | 7.36ms/12.96ms → 7.96ms/10.97ms | 1.07x |
| 20   | 2.17M  | 5.06s          | 63.29s/47.15s/11.48s | 22.91ms/335.31ms → 16.67ms/22.32ms | 9.23x | 15.90ms/80.32ms → 17.73ms/23.98ms | 2.27x | 14.59ms/30.93ms → 15.47ms/28.91ms | 1.04x |
| 40   | 4.33M  | 12.65s         | 233.60s/189.47s/52.62s | 41.22ms/1614.51ms → 45.64ms/49.73ms | 17.39x | 32.05ms/272.80ms → 37.40ms/52.56ms | 3.46x | 30.33ms/78.16ms → 36.70ms/57.35ms | 1.27x |

**Benchmark commands**:
```bash
uv run main.py benchmark --days 1 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 5 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 20 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 40 --price-update-frequency 2min --num-users 100 --num-products 500
```

#### Section 2: Varying Price Update Frequency

**Fixed parameters**: 10 trading days, 100 users, 500 products

| Frequency | Events | Bucket Refresh | Cache Refresh | Queries 15min (UP/U) | Speedup | Queries 1h (UP/U) | Speedup | Queries 1d (UP/U) | Speedup |
|-----------|--------|----------------|---------------|----------------------|---------|-------------------|---------|-------------------|---------|
| 2min      | 1.08M  | 5.95s          | 46.78s/15.20s/3.44s | 13.20ms/97.12ms → 9.48ms/13.47ms | 4.83x | 8.34ms/26.75ms → 8.09ms/17.25ms | 1.46x | 8.04ms/19.73ms → 8.16ms/10.53ms | 1.39x |
| 5min      | 433k   | 1.14s          | 9.76s/4.23s/1.53s | 6.00ms/35.53ms → 4.88ms/6.85ms | 3.54x | 3.81ms/9.80ms → 3.70ms/4.76ms | 1.48x | 3.33ms/5.67ms → 3.40ms/4.65ms | 1.12x |
| 15min     | 144k   | 0.80s          | 2.94s/1.50s/0.69s | 3.71ms/20.67ms → 3.47ms/2.73ms | 3.93x | 2.60ms/4.44ms → 2.51ms/1.98ms | 1.56x | 2.24ms/3.01ms → 1.67ms/2.23ms | 1.30x |
| 1h        | 36k    | 0.23s          | 0.31s/0.38s/0.10s | 1.50ms/4.52ms → 1.25ms/1.26ms | 2.13x | 1.10ms/1.53ms → 1.01ms/1.05ms | 1.27x | 0.96ms/1.31ms → 0.86ms/0.96ms | 1.29x |

**Benchmark commands**:
```bash
uv run main.py benchmark --days 10 --price-update-frequency 30sec --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 1min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 5min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 15min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 1h --num-users 100 --num-products 500
```

#### Section 3: Varying Number of Events

**Fixed parameters**: 2min price update frequency, 100 users, 500 products (days calculated)

| Events | Days | Bucket Refresh | Cache Refresh | Queries 15min (UP/U) | Speedup | Queries 1h (UP/U) | Speedup | Queries 1d (UP/U) | Speedup |
|--------|------|----------------|---------------|----------------------|---------|-------------------|---------|-------------------|---------|
| 50k    | 0.46 | 0.08s          | 0.20s/0.10s/0.09s | 1.49ms/2.36ms → 1.53ms/1.55ms | 1.23x | 1.11ms/1.32ms → 1.21ms/1.28ms | 1.00x | 1.15ms/1.30ms → 1.17ms/1.34ms | 0.97x |
| 100k   | 0.92 | 0.16s          | 0.46s/0.22s/0.14s | 2.04ms/3.67ms → 2.01ms/2.10ms | 1.44x | 1.62ms/2.00ms → 1.65ms/1.79ms | 1.04x | 1.71ms/2.10ms → 1.56ms/1.78ms | 1.13x |
| 500k   | 4.62 | 0.81s          | 8.20s/2.58s/1.12s | 6.84ms/25.49ms → 4.79ms/6.59ms | 2.47x | 3.97ms/8.89ms → 3.97ms/5.22ms | 1.23x | 3.96ms/5.83ms → 3.93ms/5.00ms | 1.09x |
| 1M     | 9.24 | 1.49s          | 22.26s/10.40s/2.93s | 13.95ms/76.46ms → 7.92ms/10.76ms | 4.20x | 7.23ms/21.84ms → 7.74ms/10.09ms | 1.45x | 6.63ms/11.96ms → 6.69ms/9.42ms | 1.18x |
| 2M     | 18.48| 3.33s          | 60.99s/48.25s/10.01s | 29.07ms/271.00ms → 16.69ms/20.42ms | 8.23x | 14.05ms/71.24ms → 15.37ms/20.86ms | 2.31x | 15.03ms/30.59ms → 14.04ms/20.83ms | 1.30x |

**Benchmark commands**:
```bash
uv run main.py benchmark --num-events 10000 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --num-events 50000 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --num-events 100000 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --num-events 500000 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --num-events 1000000 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --num-events 2000000 --price-update-frequency 2min --num-users 100 --num-products 500
```

#### Section 4: Varying Number of Users

**Fixed parameters**: 10 trading days, 2min price update frequency, 500 products

| Users | Events | Bucket Refresh | Cache Refresh | Queries 15min (UP/U) | Speedup | Queries 1h (UP/U) | Speedup | Queries 1d (UP/U) | Speedup |
|-------|--------|----------------|---------------|----------------------|---------|-------------------|---------|-------------------|---------|
| 50    | 1.08M  | 2.08s          | 24.83s/11.57s/3.80s | 15.40ms/149.31ms → 8.40ms/15.19ms | 7.00x | 7.33ms/41.34ms → 8.77ms/17.43ms | 1.99x | 7.56ms/18.13ms → 8.04ms/13.29ms | 1.22x |
| 100   | 1.08M  | 1.52s          | 21.68s/10.36s/2.96s | 13.51ms/84.52ms → 9.90ms/11.07ms | 4.42x | 7.64ms/25.69ms → 7.75ms/13.18ms | 1.59x | 7.53ms/14.81ms → 7.41ms/11.76ms | 1.20x |
| 500   | 1.08M  | 1.45s          | 22.54s/10.24s/3.00s | 10.54ms/29.92ms → 8.77ms/9.17ms | 2.20x | 7.41ms/10.38ms → 7.67ms/8.35ms | 1.12x | 7.32ms/8.33ms → 7.32ms/7.68ms | 1.04x |
| 1000  | 1.08M  | 1.39s          | 23.36s/10.54s/3.23s | 13.33ms/27.07ms → 9.08ms/8.96ms | 1.82x | 7.68ms/9.28ms → 7.49ms/7.87ms | 1.12x | 7.22ms/8.03ms → 7.98ms/8.07ms | 0.97x |
| 5000  | 1.08M  | 1.42s          | 34.46s/14.95s/3.50s | 14.79ms/23.57ms → 9.89ms/9.43ms | 1.73x | 8.02ms/8.81ms → 8.01ms/8.23ms | 1.04x | 8.30ms/7.99ms → 8.89ms/8.82ms | 0.95x |

**Benchmark commands**:
```bash
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 10 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 50 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 500 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 1000 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 5000 --num-products 500
```

#### Section 5: Varying Number of Products

**Fixed parameters**: 10 trading days, 2min price update frequency, 100 users

| Products | Events | Bucket Refresh | Cache Refresh | Queries 15min (UP/U) | Speedup | Queries 1h (UP/U) | Speedup | Queries 1d (UP/U) | Speedup |
|----------|--------|----------------|---------------|----------------------|---------|-------------------|---------|-------------------|---------|
| 50       | 108k   | 0.17s          | 2.03s/0.90s/0.26s | 3.20ms/9.70ms → 2.43ms/2.37ms | 2.57x | 1.97ms/3.44ms → 1.88ms/2.16ms | 1.47x | 1.74ms/2.31ms → 1.69ms/1.92ms | 1.12x |
| 100      | 217k   | 0.28s          | 3.61s/1.73s/0.49s | 19.64ms/17.66ms → 3.08ms/3.06ms | 3.05x | 3.01ms/5.87ms → 2.36ms/3.07ms | 1.45x | 2.75ms/3.86ms → 2.05ms/2.63ms | 1.44x |
| 500      | 1.08M  | 1.31s          | 22.91s/10.80s/2.96s | 12.42ms/91.06ms → 10.90ms/11.25ms | 4.62x | 8.46ms/31.32ms → 7.74ms/13.63ms | 1.66x | 8.12ms/15.37ms → 7.41ms/12.34ms | 1.21x |
| 1000     | 2.17M  | 2.87s          | 49.38s/21.27s/6.54s | 16.76ms/183.65ms → 16.03ms/22.95ms | 6.13x | 14.51ms/52.01ms → 14.96ms/28.75ms | 1.55x | 14.36ms/29.88ms → 15.42ms/25.04ms | 1.14x |
| 2000     | 4.33M  | 7.24s          | 142.06s/83.92s/24.74s | 34.40ms/351.64ms → 42.96ms/61.92ms | 4.99x | 28.87ms/106.47ms → 34.24ms/97.69ms | 1.02x | 28.20ms/62.90ms → 35.80ms/63.86ms | 0.99x |

**Benchmark commands**:
```bash
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 10
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 50
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 100
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 500
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 1000
uv run main.py benchmark --days 10 --price-update-frequency 2min --num-users 100 --num-products 2000
```

**Table abbreviations**:
- **Bucket Refresh**: Time to refresh continuous aggregates for all 3 granularities (15min/1h/1d)
- **Cache Refresh**: Time to refresh cache tables for all 3 granularities (15min/1h/1d)
- **Queries (UP/U)**: Query times before → after cache, format: "XmsUP/YmsU → AmsUP/BmsU"
  - **UP** = User-Product query (single user, single product timeline)
  - **U** = User query (single user, all products aggregated)
- **Speedup**: Average speedup ratio (before cache / after cache) for both query types

### Insights

**Cache effectiveness at 15min granularity:**

- **Dramatic speedup for user queries** (aggregated across all products)
  - 40 days of data: 1614ms → 50ms (32x faster!)
  - 2M events: 271ms → 20ms (13x faster!)
  - 50 users: 149ms → 15ms (10x faster!)
- **As dataset grows, cache provides increasingly valuable speedup** for complex aggregations
- **User-product queries** show moderate speedup (1.5-2x), already fast due to bucketing

**Multi-granularity performance:**

- **15min granularity**: Best for real-time monitoring
  - Cache speedup shines on user aggregation queries (4-17x at scale)
  - Includes real-time tier for latest data after last bucket
- **1h granularity**: Balanced performance
  - Moderate cache speedup (1.5-3.5x)
  - No real-time tier, relies on hourly buckets
- **1d granularity**: Fastest base queries
  - Minimal cache benefit (already very fast due to aggressive bucketing)
  - Best for long-term historical analysis

**Scaling observations:**

- **Bucket refresh scales linearly**: 0.08s @ 50k events → 12.65s @ 4.33M events
- **Cache refresh shows superlinear growth** at larger scales
  - 15min: 0.20s @ 50k → 233s @ 4.33M events
  - 1h: 0.10s @ 50k → 189s @ 4.33M events
  - 1d: 0.09s @ 50k → 53s @ 4.33M events
- **User query performance degrades significantly without cache** as data grows
  - Without cache: Linear degradation with data size
  - With cache: Stays consistently fast regardless of historical data volume

**User/Product count impact:**

- **More users → cache provides better speedup** (more aggregation benefit)
  - 50 users: 10x speedup on user queries
  - 5000 users: 2.5x speedup (diminishing returns as user queries become simpler)
- **More products → proportionally more events** but consistent per-product performance
  - User-product queries: Stable ~10-40ms across product counts
  - User aggregation queries: Scale with product count (more products to aggregate)

**Production readiness:**

- **Sweet spot**: 500-1000 products, 100-500 users, ~1M events
  - Sub-20ms queries after cache
  - Cache refresh: 20-50s (practical for hourly/daily refresh)
- **At scale (2000 products, 4.33M events)**:
  - Queries: 40-60ms (still responsive)
  - Cache refresh: 2-4 minutes (practical for daily refresh)
- **Recommendation**: Use 15min granularity for recent analysis, 1h/1d for historical trends

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
- ✅ Multi-granularity continuous aggregates: 15min, 1h, 1d (87-99% data reduction)
- ✅ Auto-refresh policies for continuous aggregates
- ✅ Cache + delta architecture for fast queries
- ✅ TimescaleDB compression on old chunks (5-10x additional reduction)
  - Automatic compression of chunks older than 7 days
  - Columnar storage with delta encoding for timestamps and prices
  - Dictionary encoding for product_ids (eliminates UUID redundancy)

**Still needed for multi-year production:**

- [ ] Data retention policies (e.g., keep raw prices for 90 days, bucketed data for 7 years)
- [ ] Monitoring and alerting for compression job failures

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
   main.py                          # Unified CLI interface
   twr/
      __init__.py                   # Package initialization
      database.py                   # TWRDatabase class
      event_generator.py            # Synthetic data generation (2-of-3 parameter model)
      benchmark.py                  # Performance benchmarking (2-of-3 parameter model)
   migrations/
      granularities.py              # Multi-granularity configuration (15min, 1h, 1d)
      01_schema.sql                 # Foundation: TimescaleDB, tables, hypertables
      02_triggers.sql               # Business logic: TWR calculation
      03_base_views.sql.j2          # Template: Query infrastructure for all granularities
      04_cache.sql.j2               # Template: Cache tables and refresh functions
      05_combined_views.sql.j2      # Template: User-facing views (cache + delta pattern)
   tests/
      test_twr.py                   # Test suite
   README.md
   STORAGE_ANALYSIS.md              # Storage projections and optimization
   pyproject.toml
```

**Note:** The `.j2` files are Jinja2 templates that generate SQL for all granularities. They are compiled at migration time using `migrations/granularities.py` configuration.

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

- [ ] **Update tests for multi-granularity system**: Extend test suite to cover all three granularities (15min, 1h, 1d)
- [ ] **Data retention policies**: Implement automatic cleanup of old data (e.g., keep raw prices for 90 days, bucketed data for 7 years)

### Future Enhancements

- [ ] Add period return column to timeline display (show price change % between events)
- [ ] Decouple money and units for cash flows (provider fees)
- [ ] Table partitioning for price table (in addition to TimescaleDB chunks)
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
