# CLI Refactoring Proposal

## Overview

This document proposes a reorganized CLI structure and code organization for the TWR calculator project. These changes are **deferred for future consideration** to avoid disrupting the current working implementation.

## Current State

### File Structure
```
/twr
  main.py                      # 600 lines: TWRDatabase class + CLI
  event_generator.py           # 325 lines: EventGenerator class + CLI
  benchmark.py                 # 298 lines: Benchmark class + CLI
  migrations/
  tests/
```

### Current CLI Commands

**main.py:**
- `drop` - Drop and recreate database
- `migrate` - Run database migrations
- `refresh` - Refresh timeline cache
- `add-price` - Add a price record
- `add-cashflow` - Add a cash flow (buy/sell)
- `show` - Display all tables and views

**event_generator.py:**
- Standalone script with `--num-events`, `--num-users`, `--num-products`

**benchmark.py:**
- Standalone script with `--num-events`, `--num-users`, `--num-products`, `--num-queries`

## Proposed Changes

### 1. Code Organization

Move library code into a Python package:

```
/twr
  main.py                      # CLI only (~200 lines)
  twr/
    __init__.py                # Package initialization
    database.py                # TWRDatabase class
    event_generator.py         # EventGenerator class
    benchmark.py               # Benchmark class
  migrations/
  tests/
```

**Benefits:**
- Cleaner separation of concerns (library vs CLI)
- Easier to import and use as a library
- Better testability
- Standard Python package structure

### 2. Unified CLI Structure

Reorganize commands into logical groups with nested subcommands:

```
main.py
├── db                         # Database management
│   ├── drop                   # Drop and recreate database
│   ├── migrate                # Run migrations
│   └── refresh                # Refresh cache
├── data                       # Add data
│   ├── price                  # Add price record
│   └── cashflow               # Add cashflow
├── query                      # Query and display
│   ├── all                    # Show everything
│   ├── prices                 # Show prices only
│   ├── cashflows              # Show cashflows only
│   ├── timeline               # Show user-product timeline
│   └── portfolio              # Show user portfolio timeline
├── generate                   # Generate synthetic data
└── benchmark                  # Run benchmarks
```

### 3. Global Database Connection Options

Add database connection parameters at the top level:

```bash
--host HOST           Database host (default: 127.0.0.1)
--port PORT           Database port (default: 5432)
--dbname DBNAME       Database name (default: twr)
--user USER           Database user (default: twr_user)
--password PASSWORD   Database password (default: twr_password)
```

Currently these are hardcoded in main.py but configurable in event_generator.py and benchmark.py.

## Proposed Help Output

### Top-level help
```
$ uv run main.py --help

╭─────────────────────────────────────────────────────────────────╮
│ TWR (Time-Weighted Return) Calculator                          │
│ PostgreSQL-based portfolio tracking with incremental caching    │
╰─────────────────────────────────────────────────────────────────╯

usage: main.py [-h] [--host HOST] [--port PORT] [--dbname DBNAME]
               [--user USER] [--password PASSWORD]
               {db,data,query,generate,benchmark} ...

Database connection options:
  --host HOST           Database host (default: 127.0.0.1)
  --port PORT           Database port (default: 5432)
  --dbname DBNAME       Database name (default: twr)
  --user USER           Database user (default: twr_user)
  --password PASSWORD   Database password (default: twr_password)

Commands:
  {db,data,query,generate,benchmark}
    db                  Database management
    data                Add data (prices, cashflows)
    query               Query and display data
    generate            Generate synthetic test data
    benchmark           Run performance benchmarks

Run 'main.py <command> --help' for more information on a command.
```

### Database management
```
$ uv run main.py db --help

usage: main.py db [-h] {drop,migrate,refresh} ...

Database management commands

Commands:
  {drop,migrate,refresh}
    drop                Drop and recreate database
    migrate             Run database migrations
    refresh             Refresh timeline cache
```

### Data commands
```
$ uv run main.py data --help

usage: main.py data [-h] {price,cashflow} ...

Add data to the database

Commands:
  {price,cashflow}
    price               Add a price record
    cashflow            Add a cash flow (buy/sell)
```

```
$ uv run main.py data price --help

usage: main.py data price [-h] --product PRODUCT --price PRICE
                          [--timestamp TIMESTAMP]

Add a price record for a product

Required:
  --product PRODUCT     Product name
  --price PRICE         Price per unit

Optional:
  --timestamp TIMESTAMP ISO timestamp (default: now)
```

```
$ uv run main.py data cashflow --help

usage: main.py data cashflow [-h] --user USER --product PRODUCT
                              (--units UNITS | --money MONEY)
                              [--timestamp TIMESTAMP]

Add a cash flow (buy or sell) for a user

Required:
  --user USER           User name
  --product PRODUCT     Product name

Cash flow amount (one required):
  --units UNITS         Units bought/sold (positive=buy, negative=sell)
  --money MONEY         Money amount (positive=buy, negative=sell)

Optional:
  --timestamp TIMESTAMP ISO timestamp (default: now)
```

### Query commands
```
$ uv run main.py query --help

usage: main.py query [-h] {all,prices,cashflows,timeline,portfolio} ...

Query and display data

Commands:
  {all,prices,cashflows,timeline,portfolio}
    all                 Show all tables and views
    prices              Show product prices
    cashflows           Show user cash flows
    timeline            Show user-product timeline
    portfolio           Show user portfolio timeline
```

### Generate command
```
$ uv run main.py generate --help

usage: main.py generate [-h] [--num-events NUM_EVENTS]
                        [--num-users NUM_USERS]
                        [--num-products NUM_PRODUCTS]
                        [--price-delta-min PRICE_DELTA_MIN]
                        [--price-delta-max PRICE_DELTA_MAX]
                        [--cashflow-min CASHFLOW_MIN]
                        [--cashflow-max CASHFLOW_MAX]
                        [--initial-price INITIAL_PRICE]
                        [--existing-product-prob EXISTING_PRODUCT_PROB]

Generate synthetic test data for benchmarking

Event configuration:
  --num-events NUM_EVENTS       Total events to generate (default: 100)
  --num-users NUM_USERS         Number of users (default: 5)
  --num-products NUM_PRODUCTS   Number of products (default: 10)

Price configuration:
  --price-delta-min PRICE_DELTA_MIN   Min price change % (default: -0.02)
  --price-delta-max PRICE_DELTA_MAX   Max price change % (default: 0.025)
  --initial-price INITIAL_PRICE       Initial price for products (default: 100.0)

Cashflow configuration:
  --cashflow-min CASHFLOW_MIN         Min cashflow amount (default: 50)
  --cashflow-max CASHFLOW_MAX         Max cashflow amount (default: 500)
  --existing-product-prob PROB        Probability of investing in existing
                                      product (default: 0.9)
```

### Benchmark command
```
$ uv run main.py benchmark --help

usage: main.py benchmark [-h] [--num-events NUM_EVENTS]
                         [--num-users NUM_USERS]
                         [--num-products NUM_PRODUCTS]
                         [--num-queries NUM_QUERIES]
                         [--skip-cache-refresh]

Run performance benchmarks

Test configuration:
  --num-events NUM_EVENTS       Number of events to generate (default: 1000)
  --num-users NUM_USERS         Number of users (default: 50)
  --num-products NUM_PRODUCTS   Number of products (default: 100)
  --num-queries NUM_QUERIES     Number of queries to sample (default: 100)

Options:
  --skip-cache-refresh          Skip cache refresh benchmark
```

## Example Usage Comparison

### Current
```bash
# Database setup
uv run main.py drop
uv run main.py migrate

# Add data
uv run main.py add-price --product AAPL --price 150.00
uv run main.py add-cashflow --user alice --product AAPL --money 10000

# Query
uv run main.py show

# Refresh cache
uv run main.py refresh

# Generate and benchmark (separate scripts)
uv run event_generator.py --num-events 10000 --num-users 100 --num-products 500
uv run benchmark.py --num-events 100000 --num-users 1000
```

### Proposed
```bash
# Database setup
uv run main.py db drop
uv run main.py db migrate

# Add data
uv run main.py data price --product AAPL --price 150.00
uv run main.py data cashflow --user alice --product AAPL --money 10000

# Query (more granular options)
uv run main.py query all
uv run main.py query timeline
uv run main.py query portfolio

# Refresh cache
uv run main.py db refresh

# Generate and benchmark (unified)
uv run main.py generate --num-events 10000 --num-users 100 --num-products 500
uv run main.py benchmark --num-events 100000 --num-users 1000

# With custom database connection
uv run main.py --host prod.db.com --dbname twr_prod query all
```

## Benefits of Proposed Structure

1. **Logical Grouping**: Related commands grouped together (`db`, `data`, `query`)
2. **Unified Interface**: All functionality accessible through one CLI
3. **Consistency**: Database connection options work for all commands
4. **Discoverability**: Nested help menus guide users to the right command
5. **Extensibility**: Easy to add new command groups as the system grows
6. **Professional**: Matches conventions of popular CLI tools (git, docker, kubectl)

## Risks & Considerations

1. **Breaking Change**: Would break existing scripts/workflows
2. **Migration Path**: Need to update documentation, examples, tests
3. **Complexity**: More nested levels might make simple commands longer
4. **Backwards Compatibility**: Could provide aliases for old commands

## Decision

**Status**: Deferred

This refactoring is well-designed but deferred to avoid disrupting the current working implementation. The current CLI is functional and familiar. This proposal is documented for future consideration when:

- The codebase grows significantly
- External users need a more polished interface
- We want to publish as a proper Python package
- Breaking changes are acceptable (major version bump)

## Implementation Notes

If implemented, key steps would be:

1. Create `twr/` package with `__init__.py`
2. Move `TWRDatabase` → `twr/database.py`
3. Move `EventGenerator` → `twr/event_generator.py`
4. Move `Benchmark` → `twr/benchmark.py`
5. Rewrite `main.py` with nested argparse subparsers
6. Update all imports in `tests/test_twr.py`
7. Update `README.md` examples
8. Consider migration period with deprecated command warnings
