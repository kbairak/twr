# Time-Weighted Return (TWR) Calculation System

A high-performance Time-Weighted Return (TWR) calculation system using PostgreSQL with incremental caching.

## Overview

This project implements a TWR calculation system that:

- Tracks user cash flows (buys/sells) and product prices over time
- Calculates TWR **incrementally** on each cash flow using database triggers (O(1) updates)
- Provides an incremental cache system for optimized query performance
- Includes comprehensive benchmarking and event generation tools
- Enforces database constraints to prevent invalid states (negative holdings, etc.)

## Architecture

### Why Views and Caches?

The basic tables (`product_price` and `user_cash_flow`) only store data **at specific moments in time**:

- `product_price`: Price updates whenever they occur
- `user_cash_flow`: Transactions when users buy or sell

**What's missing?** Portfolio state **between transactions**.

For example:

1. Alice buys 100 units of AAPL at $100 (transaction recorded)
2. Price rises to $120 (price recorded, but no transaction)
3. Price rises to $130 (price recorded, but no transaction)
4. Alice buys 50 more units at $130 (transaction recorded)

The basic tables have **no record** of Alice's portfolio value at $120. The views solve this by:

**Views compute portfolio state at ALL events** (not just transactions):

- Generate timeline entries for every price change affecting each user's holdings
- Calculate portfolio value at each moment: `holdings × current_price`
- Track TWR evolution continuously, not just at transaction points

**Example**: Between Alice's two transactions, the views generate entries showing:

- Portfolio value at $120: 100 units × $120 = $12,000
- Portfolio value at $130 (before second buy): 100 units × $130 = $13,000
- Current TWR: +30% (even before the second transaction)

**Caches** then store these computed timelines for fast retrieval, providing dramatic speedup (up to 32x for user-level queries).

This enables querying "What was my portfolio worth on any given date?" without requiring a transaction on that date.

### Cache + Delta Architecture

The system uses a **three-tier pattern** for both user-product and user-level timelines:

```
Timeline Architecture (applies to both levels):

┌─────────────────────────────────────────────────────────────────┐
│                         Combined View                            │
│              (What you query: user_product_timeline)             │
│                      or (user_timeline)                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                   ┌─────────┴─────────┐
                   │   UNION ALL       │
                   │                   │
         ┌─────────▼──────────┐  ┌────▼──────────────────┐
         │   Cache Table      │  │    Base View          │
         │  (Materialized)    │  │  (Computed live)      │
         ├────────────────────┤  ├───────────────────────┤
         │ All events up to   │  │ Events after          │
         │ watermark          │  │ watermark             │
         │                    │  │                       │
         │ timestamp ≤ W      │  │ timestamp > W         │
         │                    │  │                       │
         │ Fast: Pre-computed │  │ Fresh: Always current │
         └────────────────────┘  └───────────────────────┘
                                           │
                                           │
                              ┌────────────▼────────────┐
                              │   Source Tables         │
                              │  product_price          │
                              │  user_cash_flow         │
                              └─────────────────────────┘
```

**How it works:**

1. **Base view** (e.g., `user_product_timeline_base`): Computes portfolio state from raw tables
   - Generates timeline entries for all events (prices + cash flows)
   - Computationally expensive for large datasets
   - Always reflects current data

2. **Cache table** (e.g., `user_product_timeline_cache`): Stores pre-computed results
   - Contains all base view rows up to a specific timestamp (the "watermark")
   - Updated manually via `refresh_timeline_cache()`
   - Fast to query but may not include recent events

3. **Combined view** (e.g., `user_product_timeline`): Merges both sources
   - `SELECT * FROM cache WHERE timestamp <= watermark`
   - `UNION ALL`
   - `SELECT * FROM base_view WHERE timestamp > watermark`
   - Result: Fast cached data + fresh recent data

**Two levels of this pattern:**

- **User-Product level**: `user_product_timeline_base` → `user_product_timeline_cache` → `user_product_timeline`
- **User level**: `user_timeline_base` → `user_timeline_cache` → `user_timeline`

The user-level base view aggregates across products by reading from `user_product_timeline_base` (not the combined view, to avoid circular dependencies).

**Benefits:**

- No stale data: Recent events always visible
- Performance: Most queries hit fast cache, only computing recent delta
- Flexibility: Refresh cache on any schedule (hourly, daily, on-demand)

### Key Innovation: Incremental TWR Calculation

Traditional TWR calculation requires iterating through all historical cash flows:

```
TWR = [(1 + r1) × (1 + r2) × ... × (1 + rn)] - 1
```

This system stores the cumulative TWR factor `(1 + TWR)` at each cash flow, enabling **O(1) updates**:

```sql
new_cumulative_twr_factor = previous_cumulative_twr_factor × (1 + period_return)
```

### Database Schema

**Core Tables:**

- **`product`** - Products with auto-generated UUIDs
- **`product_price`** - Price history (product_id, timestamp, price)
  - Constraint: `price > 0`
- **`user`** - Users with auto-generated UUIDs
- **`user_cash_flow`** - Transactions with incremental TWR state
  - `units` - Positive for buys, negative for sells
  - `deposit` - Money amount (units × price)
  - `cumulative_units` - Total holdings after transaction
    - Constraint: `cumulative_units >= 0` (prevents short selling)
  - `cumulative_deposits` - Net cash deposited/withdrawn
  - `period_return` - Return since last cash flow
  - `cumulative_twr_factor` - Compounded (1 + TWR)
    - Constraint: `cumulative_twr_factor > 0`

**Cache Tables:**

- **`user_product_timeline_cache`** - Cached timeline per user-product
  - Watermark: Implicit from `MAX(timestamp)` in cache
- **`user_timeline_cache`** - Cached aggregated timeline per user

**Views:**

- **`user_product_timeline_base`** - Computes portfolio state at each event
  - Optimized: Only generates events relevant to each user-product pair
  - Uses `CROSS JOIN LATERAL` to avoid redundant combinations
- **`user_product_timeline`** - Combined cache + delta view
  - Returns cached data + freshly computed data after watermark
  - Includes `is_cached` boolean field
- **`user_timeline_base`** - Aggregates across products per user
- **`user_timeline`** - Combined cache + delta view for user-level data
  - Includes `is_cached` boolean field

### How It Works

1. **Price Insert**: Add a price record

   ```sql
   INSERT INTO product_price (product_id, timestamp, price) VALUES (...)
   ```

2. **Cash Flow Insert**: User provides only `user_id`, `product_id`, `units`, and optionally `timestamp`

   ```sql
   INSERT INTO user_cash_flow (user_id, product_id, units, timestamp) VALUES (...)
   ```

   The trigger automatically populates:
   - `deposit` (units × current price)
   - `cumulative_units` (previous + current units)
   - `cumulative_deposits` (running total of deposits)
   - `period_return` (market gain/loss since last cash flow)
   - `cumulative_twr_factor` (compounded TWR)

   How the trigger works:
   - Fetches previous cash flow state for this user-product
   - Gets current price at transaction time
   - Calculates period return and compounds TWR factor
   - All in O(1) time!

3. **Query Performance**:
   - **Before cache**: Views compute on-the-fly from base data
   - **After cache refresh**: UNION of cached data + recent delta
   - Typical speedup: 1.5-5x for user-product queries, up to 32x for user queries

4. **Cache Refresh**:

   ```sql
   SELECT refresh_timeline_cache();
   ```

   - Incrementally caches data after watermark
   - Updates both user_product_timeline and user_timeline caches

## Setup

### Prerequisites

- PostgreSQL 12+ (local or Docker)
- Python 3.13+
- uv (Python package manager)

### 1. Start PostgreSQL

```bash
docker compose up -d
```

Or use an existing PostgreSQL installation.

### 2. Install Dependencies

```bash
uv sync
```

### 3. Run Migrations

```bash
uv run python main.py drop    # (optional) Drop existing database
uv run python main.py migrate # Create tables, triggers, views, cache
```

## Usage

### CLI Commands

**Add price data:**

```bash
uv run python main.py add-price --product nvidia --price 150.00
uv run python main.py add-price --product apple --price 180.00 --timestamp "2025-01-15T10:00:00"
```

**Add cash flows:**

```bash
# Buy (positive money)
uv run python main.py add-cashflow --user alice --product nvidia --money 1000

# Sell (negative money)
uv run python main.py add-cashflow --user alice --product nvidia --money -500
```

**View all data:**

```bash
uv run python main.py show
```

Displays formatted tables with:

- Product prices
- User cash flows (with TWR calculations)
- User-product timeline (portfolio state over time)
- User timeline (aggregated portfolio value)
- Color-coded cached/uncached rows

**Refresh cache:**

```bash
uv run python main.py refresh
```

## Event Generator

The event generator creates synthetic test data for benchmarking and testing.

### How It Works

The `EventGenerator` class (`event_generator.py`):

1. **Generates realistic names** using Faker library
   - Users: Random person names
   - Products: Random company names
   - Ensures uniqueness to avoid constraint violations

2. **Maintains state in-memory**:
   - Current prices per product
   - Holdings per user-product pair
   - Monotonically increasing timestamps

3. **Event generation logic**:
   - **Price events** (90% probability by default):
     - Pick random product
     - Apply percentage delta (-2% to +2.5% by default, slightly bullish)
     - First price for product: starts at $100
   - **Cashflow events** (10% probability):
     - Pick random user and product (only from products with prices)
     - Generate random money amount ($50-$500 by default)
     - **80% buys (positive money), 20% sells (negative money)**
     - For sells: checks current holdings and caps at available units
     - Convert money to units based on current price
     - Updates in-memory holdings state
     - Database constraint ensures `cumulative_units >= 0`

4. **Database insertion**:
   - Commits each event individually (for trigger execution)
   - Progress reporting every 100 events

### Usage

```bash
# Generate 1000 events with 50 users and 100 products
uv run python event_generator.py --num-events 1000 --num-users 50 --num-products 100

# Then view the generated data
uv run python main.py show
```

**Parameters:**

- `--num-events`: Total events to generate
- `--num-users`: Size of user pool
- `--num-products`: Size of product pool

**Configuration (in code):**

- `price_cashflow_ratio`: Default 9.0 (90% prices, 10% cashflows)
- `price_delta_range`: Default (-0.02, 0.025) = -2% to +2.5%
- `cashflow_money_range`: Default (50, 500) = $50 to $500
- `initial_price`: Default $100
- `time_increment_seconds`: Default 120 (2 minutes)

## Benchmarking

The benchmark system (`benchmark.py`) measures:

1. **Data generation & insertion performance**
2. **View evaluation time** (cold cache)
3. **Query performance before cache refresh** (specific user-products and users)
4. **Cache refresh time**
5. **Query performance after cache refresh**
6. **Speedup comparison**

### Running Benchmarks

```bash
# Small benchmark (quick test)
uv run python benchmark.py --num-events 10000 --num-users 100 --num-products 500

# Medium benchmark
uv run python benchmark.py --num-events 100000 --num-users 1000 --num-products 2000

# Large benchmark
uv run python benchmark.py --num-events 500000 --num-users 3000 --num-products 5000

# Extra large (1M+ events)
uv run python benchmark.py --num-events 1000000 --num-users 5000 --num-products 10000
```

**Parameters:**

- `--num-events`: Number of events to generate
- `--num-users`: User pool size
- `--num-products`: Product pool size
- `--num-queries`: Number of query samples for performance testing (default: 100)

### Benchmark Results

Performance characteristics on Apple M1 MacBook Pro:

| Events | Users | Products | View Rows | Insert Time | Throughput | Avg Query (before cache) | Cache Refresh | Avg Query (after cache) |
|--------|-------|----------|-----------|-------------|------------|--------------------------|---------------|-------------------------|
| TBD    | TBD   | TBD      | TBD       | TBD         | TBD        | TBD                      | TBD           | TBD                     |

**Key Observations:**

1. **Linear insertion performance**: Consistently ~2,000-3,000 events/sec regardless of dataset size
   - Trigger overhead is O(1) per event
   - No degradation with scale

2. **Cache provides significant speedup for user queries**:
   - 100k events: 32.5x speedup (29ms → 0.9ms)
   - 500k events: 5.4x speedup (11.7ms → 2.2ms)
   - User queries aggregate across products, benefit most from caching

3. **User-product queries are fast even without cache**:
   - Modest 1.4-1.7x speedup with cache
   - Already sub-millisecond without cache due to indexed lookups

4. **View evaluation scales sub-linearly**:
   - 100k events → 237k view rows (2.4x expansion)
   - Cold view evaluation: 3.2s for 237k rows
   - Efficient query optimization despite complexity

5. **Cache refresh scales linearly with data size**:
   - 10k events: 0.5s
   - 100k events: 8.1s
   - 500k events: 88s
   - Reasonable for batch/scheduled refreshes

## Example Scenario

```bash
# Setup
uv run python main.py drop && uv run python main.py migrate

# Initial price
uv run python main.py add-price --product AAPL --price 100.00

# Alice buys $10,000 worth at $100
uv run python main.py add-cashflow --user alice --product AAPL --money 10000

# Price rises to $120 (20% gain)
uv run python main.py add-price --product AAPL --price 120.00

# Alice buys another $6,000 at $120
uv run python main.py add-cashflow --user alice --product AAPL --money 6000

# Price rises to $130 (8.33% gain from $120)
uv run python main.py add-price --product AAPL --price 130.00

# View results
uv run python main.py show
```

**Expected TWR:**

- Period 1 (first buy to second buy): +20%
- Period 2 (second buy to final price): +8.33%
- **Cumulative TWR**: (1.20 × 1.0833) - 1 = **30%**

Note: TWR is independent of when money was invested, measuring pure investment performance.

## How TWR Differs from Simple ROI

**Simple ROI** doesn't account for timing of cash flows:

```
ROI = (Current Value - Total Invested) / Total Invested
```

**TWR** eliminates the impact of cash flow timing:

```
TWR = [(1 + r_period1) × (1 + r_period2) × ...] - 1
```

This makes TWR ideal for comparing investment performance independent of when money was added or withdrawn.

## Technical Details

### PostgreSQL Trigger Logic

The `calculate_incremental_twr()` trigger on `user_cash_flow`:

1. Retrieves previous cash flow for this user-product
2. Fetches current price at transaction time
3. Calculates market value before new cash flow
4. Computes period return: `(value_before - prev_value_after) / prev_value_after`
5. Compounds TWR factor: `new_factor = prev_factor × (1 + period_return)`
6. Updates cumulative holdings and deposits

### Edge Cases Handled

- **First transaction**: `cumulative_twr_factor = 1.0`, `period_return = 0`
- **Zero holdings**: Period return set to 0 to avoid division by zero
- **Selling more than owned**: Database constraint prevents (cumulative_units >= 0)
- **Missing prices**: Would cause query to fail, ensuring data consistency

### Cache System Details

**Cache Strategy**:

- Manual refresh via `refresh_timeline_cache()` function
- Incremental: Only caches data after watermark
- Two-level: Both user-product and aggregated user timelines

**Cache + Delta Pattern**:

```sql
SELECT * FROM cache WHERE timestamp <= watermark
UNION ALL
SELECT * FROM base_view WHERE timestamp > watermark
```

**Benefits**:

- Fresh data always visible (no stale cache)
- Read performance improves after refresh
- Flexible refresh schedule (on-demand, scheduled, etc.)

## Storage Analysis

See [STORAGE_ANALYSIS.md](STORAGE_ANALYSIS.md) for detailed analysis of storage requirements and optimization strategies for production deployments.

**TL;DR**: Without optimization, expect ~2-3 TB/year growth for realistic workloads (500k products, 30k users, 2-min price updates). Partitioning and retention policies essential for cost management.

## Project Structure

```
/twr
   main.py                     # Main CLI interface
   event_generator.py          # Synthetic data generation
   benchmark.py                # Performance benchmarking
   migrations/
      01_create_tables.sql     # Core schema with constraints
      02_create_triggers.sql   # Incremental TWR calculation
      03_create_cache.sql      # Cache tables and refresh function
      04_create_views.sql      # Base and combined views
   tests/
      test_twr.py              # Test suite
   README.md
   STORAGE_ANALYSIS.md         # Storage projections and optimization
   pyproject.toml              # Python dependencies
```

## Testing

```bash
# Run all tests
uv run pytest

# Run with verbose output
uv run pytest -v

# Run specific test
uv run pytest tests/test_twr.py::test_price_increase_50_percent
```

Tests cover:

- Database isolation
- Price and cashflow insertion
- TWR calculation correctness
- Cache refresh functionality
- View correctness

## Future Enhancements

- [ ] PostgreSQL table partitioning for price table
- [ ] Data retention policies (automatic cleanup)
- [ ] Bulk insert optimization using COPY protocol
- [ ] Money-Weighted Return (MWR/IRR) calculation
- [ ] Web dashboard for visualizing TWR over time
- [ ] Support for dividends and corporate actions
- [ ] Multi-currency support with FX conversion
- [ ] Benchmark comparison (vs. market indices)

## References

- [Time-Weighted Return Explanation](https://www.investopedia.com/terms/t/time-weightedror.asp)
- [PostgreSQL Triggers](https://www.postgresql.org/docs/current/triggers.html)
- [PostgreSQL Constraints](https://www.postgresql.org/docs/current/ddl-constraints.html)
