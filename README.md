# Portfolio Management System

## Design

This system is designed around 3 main processes:

- You enter raw data on one end: price updates and cashflow events (buys/sells)
- You set up periodic tasks to refresh caches in order to speed up queries
- You query investment performance data on the other end: per investment (user-product) and per user

## Data model

This system starts from simple data and builds layers on top of layers until it can provide meaningful aggregate information for an investment service.

### Raw data: Cashflows

Fields:

- `id` (for idempotency checks)
- `user_id`
- `product_id`
- `timestamp`
- `units_delta`
- `execution_price`
- `user_money`

and the following values can be easily derived:

- `execution_money = units_delta × execution_price`
- `fees = user_money - execution_money`
- `user_price = user_money / units_delta`

Notes:

1. Cashflows represent deltas, not cumulative totals. They are data related to a single transaction. The fact that we save single transactions allows us to support out-of-order inserts while still being able to compute cumulative totals later, via mechanisms that will be explained later
2. We need to explain what the `execution_` and `user_` prefixes mean: `execution_money` is the amount that the provider _claims_ they converted into/from units at `execution_price` units of currency per unit. `user_money` is the amount that the user actually lost or gained. This allows us to calculate fees, both individually and cumulatively
3. The `user_id`, `product_id` and `timestamp` combination is **not** unique; the system allows for different cashflows to share these values and they will be properly aggregated later

### Raw Data: Price updates, time buckets and granularities

We use timescaledb continuous aggregates to create time buckets for price updates. This allows us to reduce the number of events we need to process when building timelines later. The various bucketing policies are configured in `migrations/granularities.json`, which allows for easy customization. The SQL code used for the timescaledb instructions and the rest of the aggregation logic is generated using Jinja2 templates which use `granularities.json` as context. Timescaledb is also used to compress the price update tables without sacrificing query performance.

The columns, both for the raw and the bucketed price updates are:

- `product_id`
- `timestamp`
- `price`

### Layering

```
               |---------------|
               | user_timeline |
               |---------------|
                       ^
                       |
           |-----------------------|
           | user_product_timeline |
           |-----------------------|
                ^             ^
                |             |
|---------------------|  |-----------------------|
| cumulative_cashflow |  | price_update_SUFFIX   |
|                     |  | (timescaledb buckets) |
|---------------------|  |-----------------------|
          ^                         ^
          |                         |
     |----------|           |--------------|
     | cashflow |           | price_update |
     |----------|           |--------------|
```

The rest of the data is built on top of the raw data via layering. Each layer has a cache table (or multiple in case of granularities), a SQL _view_ function and a refresh function. The refresh function is meant to be called periodically to help speed up queries. The view function is written in such a way so that it will return the same data regardless of whether the cache was recently refreshed or not. The cache and the view function are codependent:

```sql
CREATE TABLE cache_table (...);

CREATE FUNCTION view() RETURNS TABLE (...) LANGUAGE plpgsql AS $$ BEGIN
    RETURN QUERY
    SELECT * FROM cache_table
    UNION ALL
    SELECT * FROM raw_data;  -- perform complex query logic
END; $$;

CREATE FUNCTION refresh() RETURNS void LANGUAGE plpgsql AS $$ BEGIN
    INSERT INTO cache_table(...)
    SELECT * from view()
    WHERE ...;
END; $$;
```

So we use the cache table to return part of the view's results and we use the view to generate the data we want to insert to the cache table during refresh.

The actual logic is more complicated, but this gives the basic idea.

### Cumulative cashflow

This layer provides cumulative running totals (units held, total invested, etc.) for cashflows over time. The fields being stored are:

Identity:

- `user_id`
- `product_id`
- `timestamp`

Monotonic totals (always increasing)

- `buy_units`: Σ(units_delta) for buys only
- `sell_units`: Σ(|units_delta|) for sells only
- `buy_cost`: Σ(execution_money) for buys
- `sell_proceeds`: Σ(|execution_money|) for sells
- `deposits`: Σ(user_money) for buys (what left bank)
- `withdrawals`: Σ(|user_money|) for sells (what entered bank)

From these we can derive:

- `units = buy_units - sell_units`
- `net_investment = deposits  - withdrawals`
- `fees = deposits  - buy_cost + withdrawals - sell_proceeds`
- `avg_buy_cost = buy_cost / buy_units`
- `cost_basis = units * avg_buy_cost`

We do not save these to save space.

This step aggregates cashflows that share the same `user_id`, `product_id` and `timestamp`.

**Example:** Using units held as a simple illustration

|                     | t1 | t2      | t3  | t4 |
|---------------------|----|---------|-----|----|
| cashflow            | 3u | 4u, -1u | -3u | 1u |
| cumulative_cashflow | 3u | 6u      | 3u  | 4u |

With periodic cache refresh, this looks like this:

- Starting condition

  |                           | t1 | t2      |
  |---------------------------|----|---------|
  | cashflow                  | 3u | 4u, -1u |
  | cumulative_cashflow-view  | 3u | 6u      |
  | cumulative_cashflow-cache |    |         |

- First cache refresh

  |                           | t1 | t2      |
  |---------------------------|----|---------|
  | cashflow                  | 3u | 4u, -1u |
  | cumulative_cashflow-view  | 3u | 6u      |
  | cumulative_cashflow-cache | 3u | 6u      |

- More cashflows

  |                           | t1 | t2      | t3  | t4 |
  |---------------------------|----|---------|-----|----|
  | cashflow                  | 3u | 4u, -1u | -3u | 1u |
  | cumulative_cashflow-view  | 3u | 6u      | 3u  | 4u |
  | cumulative_cashflow-cache | 3u | 6u      |     |    |

- Second cache refresh

  |                           | t1 | t2      | t3  | t4 |
  |---------------------------|----|---------|-----|----|
  | cashflow                  | 3u | 4u, -1u | -3u | 1u |
  | cumulative_cashflow-view  | 3u | 6u      | 3u  | 4u |
  | cumulative_cashflow-cache | 3u | 6u      | 3u  | 4u |

### User-product timeline

This layer is meant to answer the following question: what happens to an investment's (user-product combination) value when the product's price changes between cashflows?

|          | t1 | t2          | t3          | t4          | t5          |
|----------|----|-------------|-------------|-------------|-------------|
| p1 (ppu) | 10 |             | 12          | 15          |             |
| u1p1 (u) |    | 1           |             |             | 2           |
| u1p1 (v) |    | 10 (1 × 10) | 12 (1 × 12) | 15 (1 × 15) | 30 (2 × 15) |

We say that a user-product-timeline entry exists:

- for every price update
- for every user that has invested in the price update's product
- sometime before the price update

We **only** create user-product-timelines on top of the bucketed price updates, not the raw ones, in order to control the storage requirements and performance of the system. To that end, we have one cache table/view function/refresh function triplet per granularity.

For that user-product-timeline entry we retrieve the relevant price update and the last cumulative cashflow before that price update and we calculate the data we need, including market value. In fact, this layer has two sub-layers:

- user-product-timeline

  Identity:
  - `user_id`
  - `product_id`
  - `timestamp`

  Timestamps used to retrieve related data
  - `price_update_timestamp`
  - `cashflow_timestamp`

- user-product-timeline-business

  Identity:
  - `user_id`
  - `product_id`
  - `timestamp`

  Copied from cumulative cashflow:
  - `buy_units`
  - `sell_units`
  - `buy_cost`
  - `sell_proceeds`
  - `deposits`
  - `withdrawals`

  Derived:
  - `units = buy_units - sell_units`
  - `net_investment = deposits - withdrawals`
  - `fees = deposits - buy_cost + withdrawals - sell_proceeds`
  - `market_value = units × price`
  - `avg_buy_cost = buy_cost / buy_units`
  - `cost_basis = units × avg_buy_cost`
  - `unrealized_returns = market_value - cost_basis`

Because performing the JOINS needed to retrieve the related cashflow and price update are performant, and in order to save space, only the first sub-layer is being cached. The second sub-layer is only served by a _view_ function. So, for every granularity, the following entities are being created:

- user_product_timeline_cache_SUFFIX (cache table)
- user_product_timeline_SUFFIX (view function)
- refresh_user_product_timeline_SUFFIX (refresh function)
- user_product_timeline_business_SUFFIX (view function with more data)

We also take care of gaps in price updates. For example:

|                       |          | t1 | t2 | t3          | t4          |
|-----------------------|----------|----|----|-------------|-------------|
| price updates         | p1 (ppu) | 10 |    | 12          | 14          |
|                       | p2 (ppu) | 20 |    | 22          | (gap)       |
| cumulative cashflows  | u1p1 (u) |    | 1  |             |             |
|                       | u1p2 (u) |    | 2  |             |             |
| user-product-timeline | u1p1 (v) |    |    | 12 (1 × 12) | 14 (1 × 14) |
|                       | u1p2 (v) |    |    | 44 (2 × 22) | ???         |

Given the rules we described for when a user-product-timeline entry should exist, there shouldn't be one for the `u2p1-t4` slot since the `p2-t4` slot is missing from price-updates. Having this gap makes the next layer (user-timeline) significantly harder to compute. For this reason, we _pretend_ there is a `p2-t4` price update with the same price as the previous one (`p2-t3`).

|                       |          | t1 | t2 | t3          | t4            |
|-----------------------|----------|----|----|-------------|---------------|
| price updates         | p1 (ppu) | 10 |    | 12          | 14            |
|                       | p2 (ppu) | 20 |    | 22          | (22, pretend) |
| cumulative cashflows  | u1p1 (u) |    | 1  |             |               |
|                       | u1p2 (u) |    | 2  |             |               |
| user-product-timeline | u1p1 (v) |    |    | 12 (1 × 12) | 14 (1 × 14)   |
|                       | u1p2 (v) |    |    | 44 (2 × 22) | 44 (2 × 22)   |

### User-timeline

This final layer aggregates data across all of the user's investments. Since it's a simple:

```sql
SELECT user_id, timestamp, SUM(deposits) AS DEPOSITS, ...
FROM user_product_timeline_business(...)
GROUP BY user_id, timestamp;
```

(and given that we took care of the gaps in the previous layer which would mess this aggregation up), we don't use a cache table for this.

Similarly to before, we have one such view function per granularity:

- user_timeline_business_15min
- user_timeline_business_1h
- user_timeline_business_1d

### Out-of-Order Inserts

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

This example covered cumulative cashflows, but the same logic applies to user-product-timeline caches as well.

### Cache retention

In the granularity configuration, each granularity has a `retention_period` field that can be NULL. If not null the following will happen:

- The view SQL functions will only return data within the retention period (e.g., last 30 days)
- The refresh SQL functions will delete data older than the retention period after inserting new data (keeping at least one per user-product to serve as seed values)

Cache retention only applies to the user-product-timeline caches. The cumulative cashflow cache keeps all data indefinitely.

This allows storage requirements and performance to remain manageable indefinitely.

### "Latest" views

Finally we have the `user_product_timeline_latest` and `user_timeline_latest` views. These do not depend on granularity. The first one gets the latest price update and cumulative cashflow for each user-product and calculates the business data, the second one calls the first internally and groups by user.

## SQL Code Guide

As mentioned before, we use Jinja2 to generate the SQL for this system. All the code is in the `migrations/` folder. `migrations/granularities.json` holds the granularity configuration and currently holds:

```json
[{"suffix": "15min",
  "interval": "15 minutes",
  "cache_retention": "7 days"},
 {"suffix": "1h",
  "interval": "1 hour",
  "cache_retention": "30 days"},
 {"suffix": "1d",
  "interval": "1 day",
  "cache_retention": null}]
```

(`interval` and `cache_retention` are strings that can be converted to SQL intervals with `'{{ g.interval }}'::interval`)

When running the migrate command:

```sh
uv run src/twr/migrate.py  # or
uv run src/twr/reset.py    # clears the database first
```

Every file in the `migrations/` folder that ends in `.sql` or `.sql.j2` will be compiled (if needed) and executed in alphabetical order. The Jinja templates have access to the granularity configuration so they can generate code per granularity as needed.

Generating separate entities per granularity is as simple as:

```sql
{% for g in GRANULARITIES %}
    CREATE TABLE user_product_timeline_cache_{{ g.suffix }} (...)

    -- ...
{% endfor %}
```

### Why SQL functions?

The initial implementation used SQL views, heavy with CTEs, for queries. This worked in the sense that the returned data was accurate. However, since we build layers on top of layers and each layer is supposed to work with or without cache, the resulting query plan was huge and Postgres had a hard time optimizing. When we added a `WHERE` clause on the outer query, Postgres would not always pass this to the subqueries so they would end up processing the entire database. Many approaches were attempted to get around this, including adding `NOT MATERIALIZED` to the CTEs, writing the CTEs as Jinja macros and inlining them etc; some of them showed improvements, but were not satisfying.

With SQL functions we can pass our filters as arguments that will be passed down to the entire stack, making sure they will be taken advantage of when necessary.

The downsides is that it is now harder to inspect the queries with `EXPLAIN` - each function's SQL code must be inspected separately - and that it is not possible to use any ORM with these functions as far as I know. This should be ok though since the schema is expected to remain relatively stable.

### Anatomy of a SQL view function

(Some parts have been intentionally left out)

```sql
CREATE OR REPLACE FUNCTION cumulative_cashflow(
        p_user_id    UUID,
        p_product_id UUID,
        p_after      TIMESTAMPTZ,
        p_before     TIMESTAMPTZ
    )
    RETURNS TABLE (
        user_id UUID, product_id UUID, "timestamp" TIMESTAMPTZ, buy_units NUMERIC(20, 6), ...
    )
    LANGUAGE plpgsql STABLE AS $$
    BEGIN
        {% macro query(filter_user, filter_product, filter_after, filter_before) %}
            RETURN QUERY
            SELECT ...
            FROM cumulative_cashflow_cache ccfc
            WHERE
                TRUE
                {% if filter_user %}    AND ccfc.user_id     =  p_user_id   {% endif %}
                {% if filter_product %} AND ccfc.product_id  =  p_product_id{% endif %}
                {% if filter_after %}   AND ccfc."timestamp" >  p_after     {% endif %}
                {% if filter_before %}  AND ccfc."timestamp" <= p_before    {% endif %}

            UNION ALL

            SELECT * FROM (
                SELECT ...
                FROM _fresh_cf(p_user_id, p_product_id, p_before) fresh_cf ...
            ) AS computed_fresh
            {% if filter_after %}WHERE computed_fresh."timestamp" > p_after{% endif %}
        {% endmacro %}

        {% for fu, fp, fa, fb in itertools.product([True, False], repeat=4) %}
            {% if loop.first %}IF{% else %}ELSIF{% endif %}
                p_user_id    IS {% if fu %}NOT{% endif %} NULL AND
                p_product_id IS {% if fp %}NOT{% endif %} NULL AND
                p_after      IS {% if fa %}NOT{% endif %} NULL AND
                p_before     IS {% if fb %}NOT{% endif %} NULL
            THEN
                {{ query(filter_user=fu, filter_product=fp, filter_after=fa, filter_before=fb) }};
        {% endfor %}
        END IF;
    END;
    $$;
```

In various places we could do `WHERE (p_user_id IS NULL OR user_id = p_user_id) AND ...` which would mean that if `p_user_id` is not provided, it will not be taken into account, otherwise the query will use it as a filter. Again, the query planner proved to be too smart for its own good, trying to accommodate both conditions at the same time and falling back to retrieving the whole table even when a filter was provided. To get around this, we use a Jinja macro to generate variations on the query, depending on which parameters are available. So, instead of the previous condition, we do:

```sql
WHERE TRUE {% if filter_user %}AND user_id = p_user_id{% endif %} ...
```

After the macro is defined, we use `itertools.product` to iterate over all combinations of provided and not provided parameters and use the macro to generate a separate query for each. If we inspect the rendered code that is actually submitted to Postgres, ie if we do `SELECT prosrc FROM pg_proc WHERE proname = 'cumulative_cashflow';`, we will get something like:

```sql
IF p_user_id IS NOT NULL AND p_product_id IS NOT NULL THEN
    -- Query that uses p_user_id and p_product_id as filters
ELSIF p_user_id IS NOT NULL AND p_product_id IS NULL THEN
    -- Query that uses only p_user_id as filter
ELSIF p_user_id IS NULL AND p_product_id IS NOT NULL THEN
    -- Query that uses only p_product_id as filter
ELSIF p_user_id IS NULL AND p_product_id IS NULL THEN
    -- Query that doesn't have filters
END IF;
```

Also notice that in the above example, we don't use a `WHERE` clause to filter the `_fresh_cf` subquery. The reason is that we provided our filters as arguments to the `_fresh_cf` function.

## Evaluation

### Event Generator

The event generator creates realistic synthetic data for testing and benchmarking.

**Parameters:**

- `--days`: Number of trading days to simulate (default: 10)
- `--price-update-frequency`: How often prices update, e.g., "2min", "5min", "1h" (default: "14min")
- `--users`: Number of users to generate (default: 1000)
- `--products`: Number of products to generate (default: 500)

**How it works:**

- **Realistic market timing**: Generates events during trading hours (9:30 AM - 4:00 PM)
- **Weekend handling**: Automatically skips Saturdays and Sundays
- **Price updates**: Synchronized across all products at specified frequency
  - Random walk: prices change by -0.5 to +0.5 per update
- **Cashflows**: Randomly distributed across the time range
  - Approximately 1 cashflow per 9 price updates (per product)
  - ~50% buys, ~50% sells (random units_delta between -0.5 and +0.5)
  - Users tend to invest in products they already own (90% probability)
  - Prevents negative holdings (retries if a sell would result in negative units)

### Benchmarks

The benchmark script measures query performance at different cache levels (0%, 25%, 50%, 75%, 100%) to evaluate the impact of caching on query performance.

**Running the benchmark:**

```bash
# Generate 10 days of data with 2min price updates
uv run python src/twr/benchmark.py --days 10 --price-update-frequency 2min --users 1000 --products 100

# Generate 5 days of data with fewer users/products
uv run python src/twr/benchmark.py --days 5 --users 500 --products 50

# Use 5min price updates with default user/product counts
uv run python src/twr/benchmark.py --days 10 --price-update-frequency 5min
```

**What the benchmark measures:**

For each run, the benchmark:

1. Drops and recreates the database schema, then runs all migrations
2. Generates and inserts events
3. Refreshes TimescaleDB continuous aggregates
4. **Queries with 0% cache** (baseline - before any caching)
5. Refreshes all caches with VACUUM ANALYZE (cumulative_cashflow + user_product_timeline for all granularities)
6. **Queries with 100% cache**
7. Tests at different cache levels by deleting data beyond specific time cutoffs:
   - Delete data after 75% time point → **query with 75% cache** (oldest 75% retained)
   - Delete data after 50% time point → **query with 50% cache** (oldest 50% retained)
   - Delete data after 25% time point → **query with 25% cache** (oldest 25% retained)

**How cache reduction works:**

- Cutoff timestamps are calculated based on the time range of the generated data
- For granularities with cache retention, the time range is limited to the retention window
- At each cache level (25%, 50%, 75%), data beyond the cutoff timestamp is deleted
- VACUUM ANALYZE runs after each deletion to update statistics for the query planner
- This simulates realistic scenarios where older data is cached and newer data is computed on-the-fly

**Example output:**

```
benchmark --days=1 --price-update-frequency=14min --products=500 --users=1000
=============================================================================

⚙️ Event generation took 0.56s

🔍 Querying with 0% cache
    - user_product_timeline_business_15min: 1.35ms
    - user_timeline_business_15min        : 19.02ms
    - user_product_timeline_business_1h   : 0.73ms
    - user_timeline_business_1h           : 6.77ms
    - user_product_timeline_business_1d   : 0.48ms
    - user_timeline_business_1d           : 2.43ms

🔄 Refreshing cache
    - refresh_cumulative_cashflow         : 0.03s
    - refresh_user_product_timeline_15min : 0.46s
    - refresh_user_product_timeline_1h    : 0.14s
    - refresh_user_product_timeline_1d    : 0.04s

🔍 Querying with 100% cache
    - user_product_timeline_business_15min: 1.31ms
    - user_timeline_business_15min        : 19.06ms
    - user_product_timeline_business_1h   : 0.72ms
    - user_timeline_business_1h           : 6.74ms
    - user_product_timeline_business_1d   : 0.37ms
    - user_timeline_business_1d           : 0.64ms

🔍 Querying 15min with 75.0% cache (cutoff: 2026-02-08T20:37:35.454403)
    - user_product_timeline_business_15min: 1.28ms
    - user_timeline_business_15min        : 19.05ms

🔍 Querying 1h    with 75.0% cache (cutoff: 2026-02-08T20:37:35.454403)
    - user_product_timeline_business_1h   : 0.72ms
    - user_timeline_business_1h           : 6.77ms

🔍 Querying 1d    with 75.0% cache (cutoff: 2026-02-08T20:37:35.454403)
    - user_product_timeline_business_1d   : 0.47ms
    - user_timeline_business_1d           : 1.64ms

🔍 Querying 15min with 50.0% cache (cutoff: 2026-02-08T02:41:43.636268)
    - user_product_timeline_business_15min: 1.29ms
    - user_timeline_business_15min        : 19.04ms

🔍 Querying 1h    with 50.0% cache (cutoff: 2026-02-08T02:41:43.636268)
    - user_product_timeline_business_1h   : 0.71ms
    - user_timeline_business_1h           : 6.74ms

🔍 Querying 1d    with 50.0% cache (cutoff: 2026-02-08T02:41:43.636268)
    - user_product_timeline_business_1d   : 0.48ms
    - user_timeline_business_1d           : 1.58ms

🔍 Querying 15min with 25.0% cache (cutoff: 2026-02-07T08:45:51.818134)
    - user_product_timeline_business_15min: 1.29ms
    - user_timeline_business_15min        : 19.36ms

🔍 Querying 1h    with 25.0% cache (cutoff: 2026-02-07T08:45:51.818134)
    - user_product_timeline_business_1h   : 0.72ms
    - user_timeline_business_1h           : 6.69ms

🔍 Querying 1d    with 25.0% cache (cutoff: 2026-02-07T08:45:51.818134)
    - user_product_timeline_business_1d   : 0.47ms
    - user_timeline_business_1d           : 1.58ms
```

> Note: Actual results will vary based on hardware, data size, and PostgreSQL configuration.

## TODOs

- [ ] Store execution money instead of execution price in cashflows
- [ ] Understand and review user-product functions
- [ ] Compress more tables
- [ ] Rewrite tests
- [ ] What if no price update for 8 days?
- [ ] rename `user_product` to `investment` everywhere
- [ ] Remove unused indexes

  ```
  Heavily Used Indexes (Hot):
  - idx_cumulative_cashflow_cache_user_product_ts: 18.2M scans, 46MB - 🔥 Most critical index
  - idx_user_product_timeline_cache_15min_user_product_ts: 681K scans, 615MB - Heavy use
  - idx_cumulative_cashflow_cache_timestamp: 622K scans, 19MB
  - idx_cumulative_cashflow_cache_product: 621K scans, 41MB - ⚠️ 0 tuples fetched!

  Unused Indexes (0 scans = wasting space):

  Your custom indexes:
  - idx_price_update_time (on all price_update chunks) - 0 scans, ~350MB total wasted
  - idx_user_product_timeline_cache_15min_product_ts - 0 scans, 96MB wasted
  - idx_user_product_timeline_cache_1h_product_ts - 0 scans, 101MB wasted
  - idx_user_product_timeline_cache_1d_product_ts - 0 scans, 8KB

  Suspicious Findings:

  1. idx_cumulative_cashflow_cache_product - 621K scans but 0 tuples_fetched
  - This suggests it's being scanned but not actually returning data
  - Might be used for existence checks only, or could be redundant
  1. idx_cashflow_timestamp - 153 scans, read 11.8M tuples but 0 fetched
  - Similar issue - scanned but not useful
  ```
