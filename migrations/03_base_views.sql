-- =============================================================================
-- BASE VIEWS: Query Infrastructure
-- =============================================================================
-- This migration creates the base view system including:
-- - TimescaleDB continuous aggregates (15-minute price bucketing)
-- - Base computation views (compute timeline from raw data)
-- Note: Combined views are created in a later migration after cache tables exist
-- =============================================================================

-- -----------------------------------------------------------------------------
-- TimescaleDB Continuous Aggregates (15-minute bucketing)
-- -----------------------------------------------------------------------------
-- Create continuous aggregate for 15-minute price buckets
-- This dramatically reduces timeline row count (93% reduction)
-- Architecture supports easy addition of 1h/1d buckets later

CREATE MATERIALIZED VIEW product_price_15min
WITH (timescaledb.continuous) AS
SELECT product_id,
       time_bucket('15 minutes', timestamp) AS bucket,
       last(price, timestamp) AS price
FROM product_price
GROUP BY product_id, time_bucket('15 minutes', timestamp)
WITH NO DATA;

-- Auto-refresh policy: refresh every 15 minutes, keeping data up to 1 minute ago fresh
SELECT add_continuous_aggregate_policy('product_price_15min',
    start_offset => INTERVAL '1 month',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '15 minutes'
);

-- Index for fast lookups by product and time range
CREATE INDEX ON product_price_15min (product_id, bucket DESC);

-- -----------------------------------------------------------------------------
-- Base Computation Views (compute from raw data)
-- -----------------------------------------------------------------------------
-- Base timeline view (15-minute granularity): Portfolio state at each point in time when something changed
-- For each user-product pair, only includes relevant events:
-- - Cash flows for that specific user-product (exact timestamps)
-- - Price changes for that product using THREE-TIER approach:
--   1. Bucketed historical data from continuous aggregate (15-min buckets)
--   2. Recent raw data with exact timestamps (after last materialized bucket)
-- This avoids creating redundant rows for products a user doesn't hold
-- This is the "base" view that computes everything - used by the cache system
CREATE VIEW user_product_timeline_base_15min AS
WITH
  -- Get user-product pairs with their first cash flow timestamp
  -- MATERIALIZED to avoid duplicate sequential scans on user_cash_flow
  user_product_first_flow AS MATERIALIZED (
      SELECT user_id, product_id, MIN(timestamp) AS first_ts
      FROM user_cash_flow
      GROUP BY user_id, product_id
  ),
  -- Find the last materialized bucket per product in the continuous aggregate
  -- This is the boundary between bucketed historical data and raw recent data
  last_materialized_bucket AS (
      SELECT product_id, MAX(bucket) AS last_bucket
      FROM product_price_15min
      GROUP BY product_id
  ),
  -- Combine cash flows and relevant price changes into events
  -- Use UNION ALL since we know there are no duplicates (cash flows and prices have different sources)
  user_product_events AS (
      -- Cash flow events with all their data
      SELECT
          user_id,
          product_id,
          timestamp,
          cumulative_units AS holdings,
          cumulative_deposits AS net_deposits,
          cumulative_twr_factor AS twr_factor_at_last_flow,
          price,  -- Price at this cash flow (already stored)
          TRUE AS is_cash_flow
      FROM user_cash_flow

      UNION ALL

      -- TIER 1: Price change events from continuous aggregate (bucketed timestamps, historical)
      -- Uses 15-minute bucketed prices for 93% row reduction
      SELECT
          uff.user_id,
          uff.product_id,
          pp.bucket AS timestamp,  -- Use bucketed timestamp
          NULL::NUMERIC AS holdings,
          NULL::NUMERIC AS net_deposits,
          NULL::NUMERIC AS twr_factor_at_last_flow,
          pp.price,  -- Price at this price change event (last price in 15min bucket)
          FALSE AS is_cash_flow
      FROM user_product_first_flow uff
      JOIN product_price_15min pp ON pp.product_id = uff.product_id
      WHERE pp.bucket > uff.first_ts  -- Only prices AFTER first flow

      UNION ALL

      -- TIER 2: Price change events from raw data (exact timestamps, recent)
      -- Captures real-time price updates that haven't been materialized yet
      SELECT
          uff.user_id,
          uff.product_id,
          pp.timestamp,  -- Use exact timestamp for real-time data
          NULL::NUMERIC AS holdings,
          NULL::NUMERIC AS net_deposits,
          NULL::NUMERIC AS twr_factor_at_last_flow,
          pp.price,
          FALSE AS is_cash_flow
      FROM user_product_first_flow uff
      JOIN product_price pp ON pp.product_id = uff.product_id
      LEFT JOIN last_materialized_bucket lmb ON lmb.product_id = pp.product_id
      WHERE pp.timestamp > uff.first_ts  -- Only prices AFTER first flow
        AND (
            -- Include if no materialized buckets exist yet (new product)
            lmb.last_bucket IS NULL
            -- Or if timestamp is after the last materialized bucket's interval
            -- Buckets are [bucket, bucket+15min), so raw data starts at bucket+15min
            OR pp.timestamp >= lmb.last_bucket + INTERVAL '15 minutes'
        )
  ),

  -- For each event, get the latest cash flow data using LATERAL join
  events_with_state AS (
      SELECT
          upe.user_id,
          upe.product_id,
          upe.timestamp,
          upe.is_cash_flow,
          upe.price AS current_price,  -- Price at this event (already known)
          -- For cash flow events, use their own data; for price events, get latest cash flow
          COALESCE(upe.holdings, cf_latest.cumulative_units) AS holdings,
          COALESCE(upe.net_deposits, cf_latest.cumulative_deposits) AS net_deposits,
          COALESCE(upe.twr_factor_at_last_flow, cf_latest.cumulative_twr_factor) AS twr_factor_at_last_flow,
          -- Price at the last cash flow (for TWR calculation)
          -- For both event types, we need the price from the PREVIOUS cash flow
          cf_latest.price AS price_at_last_flow
      FROM user_product_events upe
      LEFT JOIN LATERAL (
          SELECT cumulative_units, cumulative_deposits, cumulative_twr_factor, price
          FROM user_cash_flow cf
          WHERE cf.user_id = upe.user_id
            AND cf.product_id = upe.product_id
            AND cf.timestamp < upe.timestamp  -- Strictly before (not <=)
          ORDER BY cf.timestamp DESC
          LIMIT 1
      ) cf_latest ON TRUE  -- Always join, not just for price events
  )

-- Final select: calculate current value and TWR from the state we've gathered
-- No need for additional price lookups - we have everything we need!
SELECT
    user_id,
    product_id,
    timestamp,
    holdings,
    net_deposits,
    current_price,
    holdings * current_price AS current_value,
    -- Real-time TWR calculation depends on event type:
    -- - For cashflow events: use cumulative_twr_factor directly (already correct)
    -- - For price events: compound TWR at last flow with price change since then
    CASE
        WHEN is_cash_flow THEN
            -- Cashflow event: twr_factor_at_last_flow is actually the cumulative TWR factor
            -- as of this cashflow (stored in user_cash_flow.cumulative_twr_factor)
            COALESCE(twr_factor_at_last_flow - 1, 0)
        WHEN
            price_at_last_flow > 0
            AND current_price IS NOT NULL
            AND twr_factor_at_last_flow IS NOT NULL
            THEN
                -- Price event: compound TWR from last cashflow with price change
                twr_factor_at_last_flow
                * (current_price / price_at_last_flow)
                - 1
        ELSE
            0
    END AS current_twr
FROM events_with_state
WHERE
    holdings IS NOT NULL
    AND holdings != 0  -- Exclude times when user has no holdings
ORDER BY user_id, product_id, timestamp;

-- Base user-level timeline (15-minute granularity): Aggregated portfolio value over time
-- Computes aggregations from user_product_timeline_base_15min (not the combined view)
CREATE VIEW user_timeline_base_15min AS
SELECT
    user_id,
    timestamp,
    SUM(net_deposits) AS total_net_deposits,
    SUM(current_value) AS total_value,
    -- Value-weighted TWR across all products
    CASE
        WHEN SUM(current_value) > 0
            THEN
                SUM(current_twr * current_value) / SUM(current_value)
        ELSE
            0
    END AS value_weighted_twr
FROM user_product_timeline_base_15min
GROUP BY user_id, timestamp
ORDER BY user_id, timestamp;
