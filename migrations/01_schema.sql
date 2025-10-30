-- =============================================================================
-- SCHEMA: Database Foundation
-- =============================================================================
-- This migration sets up the core database schema including:
-- - TimescaleDB extension
-- - Core tables (product, product_price, user, user_cash_flow)
-- - Hypertable conversion for time-series optimization
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TimescaleDB Extension
-- -----------------------------------------------------------------------------
-- Enable TimescaleDB extension
-- This must be the first step before creating any tables
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- -----------------------------------------------------------------------------
-- Core Tables
-- -----------------------------------------------------------------------------

-- Table to track unique products and auto-assign product_id
CREATE TABLE product (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" TEXT NOT NULL
);

-- Table 1: Product Prices
CREATE TABLE product_price (
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    price NUMERIC(20, 6) NOT NULL,

    PRIMARY KEY (product_id, timestamp)
);

-- Table to track unique users and auto-assign user_id
CREATE TABLE "user" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" TEXT NOT NULL
);

-- Table 2: User Cash Flows with TWR tracking
CREATE TABLE user_cash_flow (
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- positive for buys, negative for sells
    units NUMERIC(20, 6) NOT NULL,

    -- price at the time of transaction
    price NUMERIC(20, 6) NOT NULL,

    -- money amount for this transaction (units * price)
    deposit NUMERIC(20, 6) NOT NULL,

    -- running total of units held after this transaction
    cumulative_units NUMERIC(20, 6),

    -- running total of net cash deposited (invested - withdrawn)
    cumulative_deposits NUMERIC(20, 6),

    -- return since last cash flow
    period_return NUMERIC(20, 6),

    -- (1 + TWR) compounded, starts at 1.0
    cumulative_twr_factor NUMERIC(20, 6),

    PRIMARY KEY (user_id, product_id, timestamp)
);

-- -----------------------------------------------------------------------------
-- TimescaleDB Hypertables
-- -----------------------------------------------------------------------------
-- Convert product_price to hypertable for time-series optimization
-- Note: user_cash_flow remains a regular table (low volume, point lookups only)

SELECT create_hypertable('product_price', 'timestamp',
    chunk_time_interval => INTERVAL '1 month',
    if_not_exists => TRUE
);

-- TODO: Add compression policy later (discuss compression strategy)
-- Example: ALTER TABLE product_price SET (timescaledb.compress, timescaledb.compress_segmentby = 'product_id');
-- Example: SELECT add_compression_policy('product_price', INTERVAL '7 days');
