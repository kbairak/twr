# Cumulative Cashflow Caching

## Design

The intended use for this system includes 3 main processes:

- You enter raw data on one end: price_updates and cashflow events (buys/sells)
- You setup periodic tasks to refresh caches in order to speed up queries
- You query investment performance data on the other end: per investment (user-product) and per user

### Layer 0: Raw data: Cashflow

We define a `cashflow` table that stores transaction events as deltas. Each row represents a buy or sell transaction with:

- `user_id`, `product_id`, `timestamp` (identity)
- `units_delta` (positive for buys, negative for sells)
- `execution_price` (price per unit)
- `execution_money` (units_delta × execution_price)
- `user_money` (what left/entered the user's bank account)
- `fees` (should equal user_money - execution_money)

The challenge: we want cumulative running totals (units held, total invested, etc.) over time.

**Example:** Using units held as a simple illustration

|          | t1 | t2 | t3  | t4 |
|----------|----|----|-----|----|
| cashflow | 3u | 4u | -3u | 1u |

### Layer 1: Cumulative Cashflow

#### View Pattern

A `cumulative_cashflow` view calculates running totals from the delta events:

|                 | t1 | t2 | t3  | t4 |
|-----------------|----|----|-----|----|
| cashflow        | 3u | 4u | -3u | 1u |
| cumulative-view | 3u | 7u | 4u  | 5u |

#### Cache Pattern

The `cumulative_cashflow_cache` table has the same schema as the view. A PostgreSQL function periodically copies from the view to the cache. The cache speeds up queries by storing pre-computed results - the view delegates to the cache for timestamps before the watermark.

**Start:**

|                  | t1 | t2 |
|------------------|----|----|
| cashflow         | 3u | 4u |
| cumulative-view  | 3u | 7u |
| cumulative-cache |    |    |

**Refresh cache:**

|                  | t1 | t2 |
|------------------|----|----|
| cashflow         | 3u | 4u |
| cumulative-view  | 3u | 7u |
| cumulative-cache | 3u | 7u |

**Add more cashflows:**

|                  | t1 | t2 | t3  | t4 |
|------------------|----|----|-----|----|
| cashflow         | 3u | 4u | -3u | 1u |
| cumulative-view  | 3u | 7u | 4u  | 5u |
| cumulative-cache | 3u | 7u |     |    |

**Refresh cache:**

|                  | t1 | t2 | t3  | t4 |
|------------------|----|----|-----|----|
| cashflow         | 3u | 4u | -3u | 1u |
| cumulative-view  | 3u | 7u | 4u  | 5u |
| cumulative-cache | 3u | 7u | 4u  | 5u |

#### Out-of-Order Inserts

This layering makes it manageable to support out-of-order cashflow inserts through automatic invalidation and repair.

**Out-of-order insert:**

|                  | t1 | t1.5 | t2 | t3  | t4 |
|------------------|----|------|----|-----|----|
| cashflow         | 3u | 2u   | 4u | -3u | 1u |
| cumulative-cache | 3u |      | 7u | 4u  | 5u |

**Invalidate cache ≥ t1.5 (via trigger):**

|                  | t1 | t1.5 | t2 | t3  | t4 |
|------------------|----|------|----|-----|----|
| cashflow         | 3u | 2u   | 4u | -3u | 1u |
| cumulative-cache | 3u |      |    |     |    |

**Repair cache (via trigger):**

|                  | t1 | t1.5 | t2 | t3  | t4 |
|------------------|----|------|----|-----|----|
| cashflow         | 3u | 2u   | 4u | -3u | 1u |
| cumulative-cache | 3u | 5u   | 9u | 6u  | 7u |

Automatic repair covers all timestamps until the watermark. The watermark is implicitly defined as the largest timestamp in the cache (not explicitly stored).

> **Edge case:** If there's only one investment, an out-of-order insert could temporarily reduce the watermark (e.g., to t1 in the example above, so there would be nothing to repair). However, in realistic production scenarios with multiple investments, other investments maintain the overall watermark, allowing the repair mechanism to work correctly for the invalidated investment.

### Layer 2: User-Product Timeline

The next layer adds market price data to track portfolio value over time. Consider a scenario with both cashflow events and price updates:

|                     | t1    | t2 | t3    | t4    | t5    | t6 |
|---------------------|-------|----|-------|-------|-------|----|
| price updates       | 10$/u |    | 12$/u | 15$/u | 11$/u |    |
| cumulative cashflow |       | 5u |       |       |       | 7u |

At cashflow events, we can calculate market value by multiplying units held by the current market price:

|                     | t1    | t2      | t3    | t4    | t5    | t6      |
|---------------------|-------|---------|-------|-------|-------|---------|
| price updates       | 10$/u |         | 12$/u | 15$/u | 11$/u |         |
| cumulative cashflow |       | 5u, 50$ |       |       |       | 7u, 77$ |

However, portfolio value changes even between transactions. The `user_product_timeline` layer creates a complete time series by including price update events:

|                       | t1    | t2  | t3    | t4    | t5    | t6  |
|-----------------------|-------|-----|-------|-------|-------|-----|
| price updates         | 10$/u |     | 12$/u | 15$/u | 11$/u |     |
| cumulative cashflow   |       | 5u  |       |       |       | 7u  |
| user_product_timeline |       | 50$ | 60$   | 75$   | 55$   | 77$ |

The `user_product_timeline` uses the same view+cache pattern as `cumulative_cashflow`. Out-of-order cashflows automatically invalidate and repair this cache layer as well.

### Layer 3: Portfolio Timeline

The final layer aggregates across all products to provide a complete portfolio view for each user. Consider a user with two investments:

**Individual investments' timeline:**

|       | t1   | t2   | t3   | t4   |
|-------|------|------|------|------|
| AAPL  | 50$  | 60$  | 55$  | 60$  |
| GOOGL | 100$ | 120$ | 110$ | 115$ |

The `user_timeline` aggregates these into a single portfolio view by summing across all products at each timestamp:

|         | t1   | t2   | t3   | t4   |
|---------|------|------|------|------|
| AAPL    | 50$  | 60$  | 55$  | 60$  |
| GOOGL   | 100$ | 120$ | 110$ | 115$ |
| overall | 150$ | 180$ | 165$ | 175$ |

At each timestamp, the portfolio value is calculated by taking the latest state of each product and summing them. The `user_timeline` uses the same view+cache pattern and is automatically invalidated and repaired when underlying data changes.

### Time Buckets

In real-world systems, price updates happen more frequently than cashflows. Because each price update results in one new timeline event **for every user that has invested in the product**, the system would quickly become unmaintainable in terms of query performance and storage.

To address this, we use TimescaleDB to create time buckets for price updates. The `user_product_timeline` and `user_timeline` layers build on top of these bucketed prices instead of raw updates. Multiple granularities are configured in `migrations/granularities.json`:

- **15min**: Real-time monitoring (7 day cache retention, `include_realtime=true`)
- **1h**: Recent analysis (30 day cache retention)
- **1d**: Long-term trends (indefinite retention)

This means that we end up with `user_product_timeline_15m`, `user_product_timeline_1h`, `user_product_timeline_1d`, `user_timeline_15m`, `user_timeline_1h` and `user_timeline_1d` views (and cache tables). We can change the configuration before applying the migrations if we want different granularities.

**Cache retention** allows older data to be pruned automatically, keeping storage manageable.

**Real-time flag:** When `include_realtime=true` (only for 15min), the view uses a two-watermark system. Beyond the standard cache watermark, there's a view watermark that separates bucketed data from raw unbucketed price updates. This allows queries that end with `ORDER BY timestamp DESC LIMIT 1` to retrieve the absolute current portfolio value without waiting for the next time bucket.

### Metrics

The timeline views provide fields for calculating investment performance metrics.

**Available fields in `user_product_timeline` (per-product level):**

- Position: `units_held`, `market_value`
- Investment: `net_investment`, `deposits`, `withdrawals`, `fees`
- Cost tracking: `buy_cost`, `buy_units`, `sell_proceeds`, `sell_units`

**Available fields in `user_timeline` (portfolio level):**

- Same fields, aggregated across all products
- `cost_basis` - sum of cost basis across all current holdings
- `sell_basis` - sum of cost basis for all sold units

**Calculable metrics:**

1. **Average cost basis** (per-product): `buy_cost / buy_units`
2. **Unrealized returns** (per-product): `market_value - (average_cost_basis × units_held)`
3. **Unrealized returns** (portfolio): `market_value - cost_basis`
4. **Realized returns** (portfolio): `sell_proceeds - sell_basis`
5. **Total returns** (portfolio): `unrealized_returns + realized_returns`

These can be queried over time to show metric evolution.

**Case study: Returns calculation**

For a specific product:

```sql
SELECT
  timestamp,
  market_value - (buy_cost / NULLIF(buy_units, 0) * units_held) AS unrealized_returns
FROM user_product_timeline_1d
WHERE user_id = %s
  AND product_id = %s
ORDER BY timestamp;
```

For entire portfolio:

```sql
SELECT
  timestamp,
  market_value - cost_basis AS unrealized_returns,
  sell_proceeds - sell_basis AS realized_returns,
  (market_value - cost_basis) + (sell_proceeds - sell_basis) AS total_returns
FROM user_timeline_1d
WHERE user_id = %s
ORDER BY timestamp;
```

## Quickstart

```sh
# Python dependencies
uv sync --all-groups

# Start database
docker compose up -d

# Run migrations
uv run src/twr/migrate.py  # or reset.py, if you make changes to the migrations

# Add sample data (optional)
uv run src/twr/generate.py --num-events 100000 --days 10 --num-users 1000 --num-products 300

# Enter Postgres interactive shell
PGPASSWORD=twr_password psql --host 127.0.0.1 twr twr_user
```

Inside the Postgres shell you can do a few things:

```sql
-- Insert raw data
INSERT INTO "user" (name) VALUES ('Alice'), ('Bob');
INSERT INTO product (name) VALUES ('AAPL'), ('GOOGL');
INSERT INTO price_update (product_id, "timestamp", price) VALUES
  (?, '2024-01-01 10:00:00', 150.00),
  (?, '2024-01-01 11:00:00', 152.00),
  (?, '2024-01-01 10:30:00', 2800.00),
  (?, '2024-01-01 11:30:00', 2825.00);
INSERT INTO cashflow (user_id, product_id, "timestamp", units_delta, execution_price, fees) VALUES
  (?, ?, '2024-01-01 10:15:00', 10, 150.00, 0.00),
  (?, ?, '2024-01-01 11:15:00', -5, 152.00, 1.00),
  (?, ?, '2024-01-01 10:45:00', 2, 2800.00, 0.15),
  (?, ?, '2024-01-01 11:45:00', -1, 2825.00, 0.00);
-- (execution_money and user_money derived by trigger)

-- Force refresh timescaledb buckets (or wait for scheduled refresh)
CALL refresh_continuous_aggregate('price_update_15min', NULL, NULL);
CALL refresh_continuous_aggregate('price_update_1h', NULL, NULL);
CALL refresh_continuous_aggregate('price_update_1d', NULL, NULL);

-- Query cumulative_cashflow (before caching)
SELECT * FROM cumulative_cashflow;
SELECT * FROM cumulative_cashflow WHERE user_id = ? AND product_id = ?;

-- Refresh cumulative_cashflow cache, then query again
SELECT refresh_cumulative_cashflow();

-- Query user_product_timeline (before caching)
SELECT * from user_product_timeline_1d;
SELECT * from user_product_timeline_1d WHERE user_id = ? AND product_id = ?;

-- Refresh user_product_timeline cache, then query again
SELECT refresh_user_product_timeline_1d();
-- (Repeat for `_1h` and `_15min`)

-- Query user_timeline (before caching)
SELECT * from user_timeline_1d;
SELECT * from user_timeline_1d WHERE user_id = ? AND product_id = ?;

-- Refresh user_timeline cache, then query again
SELECT refresh_user_timeline_1d();
-- (Repeat for `_1h` and `_15min`)
```

Also

```sh
uv run pytest  # Run tests
uv run src/twr/benchmarks.py -h  # Run benchmarks
```

## Evaluation

### Event Generator

The event generator creates realistic synthetic data for testing and benchmarking using a **2-of-3 parameter model**.

**Flexible parameter model:**

Provide **any 2 of these 3 parameters**, and the third is calculated automatically:

1. `--days`: Number of trading days to simulate
2. `--num-events`: Total number of events to generate
3. `--price-update-frequency`: How often prices update (e.g., "2min", "5min", "1h")

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

### Benchmarks

The benchmark script measures query performance at different cache levels (0%, 25%, 50%, 75%, 100%) to evaluate the impact of caching on query performance.

**Running the benchmark:**

```bash
# Generate 10 days of data
PYTHONPATH=src uv run python -m twr.benchmark --days 10 --price-update-frequency 2min --num-users 1000 --num-products 100

# Generate specific number of events
PYTHONPATH=src uv run python -m twr.benchmark --days 5 --num-events 100000 --num-users 500 --num-products 50

# Generate 50k events with 5min price updates
PYTHONPATH=src uv run python -m twr.benchmark --num-events 50000 --price-update-frequency 5min
```

**What the benchmark measures:**

For each run, the benchmark:

1. Clears existing data
2. Generates and inserts events
3. Refreshes TimescaleDB continuous aggregates
4. **Queries with 0% cache** (baseline - before any caching)
5. Refreshes all caches with VACUUM ANALYZE (cumulative_cashflow + user_product_timeline for all granularities)
6. **Queries with 100% cache**
7. Progressively deletes cache and queries:
   - Delete >= 75th percentile timestamp → **query with 75% cache** (oldest 75% retained)
   - Delete >= 50th percentile timestamp → **query with 50% cache** (oldest 50% retained)
   - Delete >= 25th percentile timestamp → **query with 25% cache** (oldest 25% retained)

**How cache reduction works:**

- Percentiles are calculated from the full cache using timestamp distribution
- Deletion is progressive: each step deletes from the remaining cache
- VACUUM ANALYZE runs after each deletion to update statistics
- This simulates realistic scenarios where older data is cached and newer data is computed on-the-fly

**Example output:**

The benchmark outputs query times for each granularity (15min, 1h, 1d) at each cache level, along with cache refresh times. This helps understand the performance tradeoffs between cache size and query speed.

*Note: Actual results will vary based on hardware, data size, and PostgreSQL configuration.*
