# Time-Weighted Return (TWR) Calculation System

A PostgreSQL-based TWR calculation system with incremental caching and O(1) trigger updates.

## Features

- **Incremental TWR calculation**: Database triggers compute TWR on each cash flow (O(1) per insert)
- **Timeline generation**: Views compute portfolio state at every price change, not just transactions
- **Cache + delta architecture**: Watermark-based incremental caching for fast queries
- **Synthetic data generation**: Event generator for testing and benchmarking
- **Performance benchmarks**: Query and cache refresh performance measurements

## Quick Start

### Prerequisites

- PostgreSQL 12+ (local or Docker)
- Python 3.13+
- uv (Python package manager)

### Setup and Example

```bash
# Start PostgreSQL
docker compose up -d

# Install dependencies
uv sync

# Setup database
uv run main.py drop && uv run main.py migrate

# Initial price and first buy
uv run main.py add-price --product AAPL --price 100.00
uv run main.py add-cashflow --user alice --product AAPL --money 10000
```

**Timeline after first buy:**

```
┃ timestamp ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃ cached ┃
│ t1        | 100.00   │    $10000.00 │ $100.00 │ $10000.00 │   0.00% │   ✗    │
```

```bash
# Price rises to $120, Alice buys $6,000 more
uv run main.py add-price --product AAPL --price 120.00
uv run main.py add-cashflow --user alice --product AAPL --money 6000
```

**Timeline after second buy:**

```
┃ timestamp ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃
│ t2        | 100.00   │    $10000.00 │ $120.00 │ $12000.00 │  20.00% │  ← price change
│ t3        | 150.00   │    $16000.00 │ $120.00 │ $18000.00 │  44.00% │  ← after buy (portfolio weighted avg)
```

```bash
# Price rises to $130
uv run main.py add-price --product AAPL --price 130.00
```

**Timeline after price change:**

```
┃ timestamp ┃ holdings ┃ net_deposits ┃ price   ┃  value    ┃ twr_pct ┃
│ t4        ┃ 150.00   │    $16000.00 │ $130.00 │ $19500.00 │  30.00% │  ← period 2: +8.33%
```

**TWR calculation**: (1.20 × 1.0833) - 1 = **30%** (independent of when money was invested)

```bash
# Refresh cache
uv run main.py refresh

# Add new price after cache refresh
uv run main.py add-price --product AAPL --price 140.00
uv run main.py show  # Notice cached vs fresh data
```

**Timeline showing cache + delta:**

```
┃ timestamp  ┃ total_value ┃ twr_pct ┃ cached ┃
│ t1         │   $10000.00 │   0.00% │   ✓    │  ← cached
│ t2         │   $12000.00 │  20.00% │   ✓    │  ← cached
│ t3         │   $18000.00 │  44.00% │   ✓    │  ← cached
│ t4         │   $19500.00 │  30.00% │   ✓    │  ← cached
│ t5         │   $21000.00 │  40.00% │   ✗    │  ← fresh (computed live)
```

The last row is computed on-the-fly because it's after the cache watermark, demonstrating the cache + delta pattern.

## How It Works

### Core Tables

- **`product`**: Product registry with auto-generated UUIDs
- **`product_price`**: Price history (product_id, timestamp, price)
- **`user`**: User registry with auto-generated UUIDs
- **`user_cash_flow`**: Transactions with incremental TWR state
  - Trigger auto-calculates: deposit, cumulative values, period return, cumulative TWR factor

### Timeline Views

The basic tables only store data at specific moments (price updates and transactions). **What about portfolio value between transactions?**

Example: Alice buys 100 units at $100. Price rises to $120 (no transaction). Price rises to $130, then she buys 50 more units.

The views solve this by generating timeline entries for **every price change** affecting each user's holdings:

- At $120: holdings = 100, value = $12,000, TWR = +20%
- At $130 (before second buy): holdings = 100, value = $13,000, TWR = +30%

This enables querying "What was my portfolio worth on any date?" without requiring a transaction.

### Value-Weighted TWR for Multi-Product Portfolios

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

### Cache + Delta Architecture

```
┌─────────────────────────────────────────┐
│       Combined View (what you query)    │
│   user_product_timeline / user_timeline │
└──────────────┬──────────────────────────┘
               │ UNION ALL
       ┌───────┴────────┐
       │                │
┌──────▼───────┐  ┌──────▼──────────┐     ┌──────────────────┐
│ Cache Table  │  │   Base View     │     │   Raw tables     │
│ (≤ watermark)│  │ (> watermark)   │ -─► │ (user cashflow,  │
│   Fast       │  │   Fresh         │     │  product prices) │
└──────────────┘  └─────────────────┘     └──────────────────┘
```

- **Base view**: Computes timeline from raw tables (expensive)
- **Cache table**: Pre-computed results up to watermark timestamp
- **Combined view**: Fast cached data + fresh recent data
- **Refresh**: `refresh_timeline_cache()` incrementally adds new data

### Incremental TWR Calculation

Traditional TWR requires iterating all historical cash flows. This system stores the cumulative TWR factor at each cash flow:

```sql
new_cumulative_twr_factor = previous_cumulative_twr_factor × (1 + period_return)
```

This enables **O(1) updates** on each insert.

## Event Generator

Generate synthetic test data for benchmarking:

```bash
# Generate 1000 events with 50 users and 100 products
uv run event_generator.py --num-events 1000 --num-users 50 --num-products 100
```

**Event generation logic:**

- 90% price events, 10% cashflow events
- Price changes: -2% to +2.5% (slightly bullish)
- Users tend to invest in products they already own (90% probability)
- 80% buys, 20% sells

## Benchmarking

### Query Performance

Performance on Apple M1 MacBook Pro:

| Events | Users | Products | Avg user-product query (before cache) | Avg user query (before cache) | Cache refresh | Avg user-product query (after cache) | Avg user query (after cache) |
|--------|-------|----------|---------------------------------------|-------------------------------|---------------|--------------------------------------|------------------------------|
| 10k    | 100   | 1k       | 0.5ms                                 | 0.6ms                         | 0.02s         | 0.5ms                                | 0.6ms                        |
| 100k   | 1k    | 500      | 2.0ms                                 | 2.4ms                         | 3.2s          | 1.9ms                                | 2.2ms                        |
| 100k   | 1k    | 1k       | 2.1ms                                 | 2.2ms                         | 1.1s          | 1.8ms                                | 1.8ms                        |
| 200k   | 1k    | 500      | 3.6ms                                 | 5.7ms                         | 5.8s          | 3.3ms                                | 3.7ms                        |
| 200k   | 2k    | 1k       | 3.3ms                                 | 3.9ms                         | 6.1s          | 3.4ms                                | 4.5ms                        |
| 300k   | 1k    | 500      | 4.9ms                                 | 9.6ms                         | 11.9s         | 5.1ms                                | 11.4ms                       |
| 300k   | 3k    | 1k       | 4.5ms                                 | 5.1ms                         | 11.6s         | 5.2ms                                | 6.1ms                        |
| 400k   | 1k    | 500      | 6.8ms                                 | 13.2ms                        | 20.7s         | 6.6ms                                | 20.8ms                       |
| 400k   | 4k    | 1k       | 6.9ms                                 | 7.4ms                         | 21.8s         | 7.3ms                                | 16.7ms                       |
| 500k   | 1k    | 500      | 7.5ms                                 | 17.2ms                        | 41.3s         | 11.5ms                               | 34.4ms                       |
| 500k   | 3k    | 500      | 8.7ms                                 | 15.1ms                        | 1m 7.5s       | 12.9ms                               | 47.1ms                       |
| 500k   | 3k    | 1k       | 10.0ms                                | 11.7ms                        | 40.5s         | 10.1ms                               | 20.6ms                       |
| 600k   | 1k    | 500      | 11.5ms                                | 31.0ms                        | 55.7s         | 11.9ms                               | 42.9ms                       |
| 600k   | 6k    | 1k       | 10.6ms                                | 12.0ms                        | 1m 4.2s       | 10.3ms                               | 27.0ms                       |
| 700k   | 1k    | 500      | 13.5ms                                | 34.1ms                        | 1m 44.8s      | 18.2ms                               | 97.0ms                       |
| 700k   | 7k    | 1k       | 12.3ms                                | 13.4ms                        | 1m 48.5s      | 16.1ms                               | 59.6ms                       |
| 800k   | 1k    | 500      | 19.9ms                                | 54.6ms                        | 3m 6.3s       | 26.5ms                               | 206.8ms                      |
| 800k   | 8k    | 1k       | 16.3ms                                | 18.6ms                        | 3m 9.4s       | 22.0ms                               | 99.5ms                       |
| 900k   | 1k    | 500      | 17.2ms                                | 70.6ms                        | 3m 48.3s      | 28.9ms                               | 285.5ms                      |
| 900k   | 9k    | 1k       | 17.7ms                                | 36.4ms                        | 3m 16.6s      | 23.1ms                               | 111.9ms                      |
| 1M     | 1k    | 500      | 17.9ms                                | 52.9ms                        | 4m 15.0s      | 31.2ms                               | 355.8ms                      |
| 1M     | 10k   | 500      | 24.8ms                                | 90.1ms                        | 18m 46.3s     | 23.4ms                               | 313.3ms                      |
| 1M     | 10k   | 1k       | 16.6ms                                | 31.2ms                        | 3m 26.9s      | 19.3ms                               | 69.2ms                       |

**Key observations:**

- Sub-20ms user-product queries up to 1M events
- User-level aggregation queries scale less favorably (grow from 0.6ms to 313ms at 1M/10k/500)
- More products generally improves performance (1M/10k/1k: 31.2ms vs 1M/10k/500: 90.1ms)
  - Fewer products means users hold more products each, requiring heavier aggregation
- Cache refresh performance varies dramatically with product count:
  - 1M/10k/500: 18m 46s (concentrated portfolios create complex aggregations)
  - 1M/10k/1k: 3m 27s (distributed portfolios are more efficient to cache)
- Post-cache query performance can be slower than pre-cache due to:
  - Larger materialized cache tables requiring more disk I/O
  - Cold cache immediately after refresh (production usage would show benefits over time)
  - Small raw tables fitting entirely in buffer cache

### Running Benchmarks

```bash
# Small benchmark
uv run benchmark.py --num-events 10000 --num-users 100 --num-products 500

# Medium benchmark
uv run benchmark.py --num-events 100000 --num-users 1000 --num-products 2000
```

## Storage & Production Scale

**This is a proof-of-concept system.** See [STORAGE_ANALYSIS.md](STORAGE_ANALYSIS.md) for detailed analysis.

**TL;DR**: At realistic production scale (500k products, 2-min price updates):

- **25 billion rows/year** = 1.52 TB/year just for prices
- Cache refresh becomes impractical beyond ~1M events without optimization
- **Requires** table partitioning and retention policies for production use
- Consider specialized time-series databases (TimescaleDB) or alternative architectures

Without optimization, the system will become unmanageable within 6-12 months at production scale.

## Database Schema

### Tables

- **`product`**: id (UUID), name
- **`product_price`**: product_id, timestamp, price
- **`user`**: id (UUID), name
- **`user_cash_flow`**: user_id, product_id, timestamp, units, price, deposit, cumulative_units, cumulative_deposits, period_return, cumulative_twr_factor
- **`cache_watermark`**: Single-row table tracking last cached timestamp
- **`user_product_timeline_cache`**: Cached timeline per user-product
- **`user_timeline_cache`**: Cached aggregated timeline per user

### Views

- **`user_product_timeline_base`**: Computes portfolio state at each event (expensive)
- **`user_product_timeline`**: Combined cache + delta (fast)
- **`user_timeline_base`**: Aggregates across products per user
- **`user_timeline`**: Combined cache + delta for user-level data

**Note:** Database constraints removed for maximum insertion performance (~2x speedup). Data consistency enforced at application level.

## Project Structure

```
/twr
   main.py                          # CLI interface
   event_generator.py               # Synthetic data generation
   benchmark.py                     # Performance benchmarking
   migrations/
      01_create_tables.sql          # Core schema
      02_create_triggers.sql        # Incremental TWR calculation
      03_create_base_views.sql      # Expensive computation views
      04_create_cache.sql           # Cache tables and refresh function
      05_create_combined_views.sql  # Cache + delta union views
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

Tests cover database isolation, TWR calculation correctness, and cache functionality.

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

## Future Enhancements

- [ ] Decouple money and units for cash flows (provider fees)
- [ ] Table partitioning for price table
- [ ] Data retention policies (automatic cleanup)
- [ ] Bulk insert optimization using COPY protocol
- [ ] Money-Weighted Return (MWR/IRR) calculation
- [ ] Web dashboard for visualizing TWR over time
- [ ] Support for dividends and corporate actions
- [ ] Multi-currency support with FX conversion

## References

- [Time-Weighted Return Explanation](https://www.investopedia.com/terms/t/time-weightedror.asp)
- [PostgreSQL Triggers](https://www.postgresql.org/docs/current/triggers.html)
