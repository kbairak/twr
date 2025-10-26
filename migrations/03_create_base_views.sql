-- Base timeline view: Portfolio state at each point in time when something changed
-- For each user-product pair, only includes relevant events:
-- - Cash flows for that specific user-product
-- - Price changes for that product (only after user's first cash flow)
-- This avoids creating redundant rows for products a user doesn't hold
-- This is the "base" view that computes everything - used by the cache system
CREATE VIEW user_product_timeline_base AS
WITH
  -- Get user-product pairs with their first cash flow timestamp
  -- MATERIALIZED to avoid duplicate sequential scans on user_cash_flow
  user_product_first_flow AS MATERIALIZED (
      SELECT user_id, product_id, MIN(timestamp) AS first_ts
      FROM user_cash_flow
      GROUP BY user_id, product_id
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

      -- Price change events (without cash flow data, will be filled in later)
      SELECT
          uff.user_id,
          uff.product_id,
          pp.timestamp,
          NULL::NUMERIC AS holdings,
          NULL::NUMERIC AS net_deposits,
          NULL::NUMERIC AS twr_factor_at_last_flow,
          pp.price,  -- Price at this price change event
          FALSE AS is_cash_flow
      FROM user_product_first_flow uff
      JOIN product_price pp ON pp.product_id = uff.product_id
      WHERE pp.timestamp > uff.first_ts  -- Only prices AFTER first flow (first flow already included above)
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
    -- Real-time TWR: compound TWR at last flow with price change since then
    CASE
        WHEN
            price_at_last_flow > 0
            AND current_price IS NOT NULL
            AND twr_factor_at_last_flow IS NOT NULL
            THEN
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

-- Base user-level timeline: Aggregated portfolio value over time
-- Computes aggregations from user_product_timeline_base (not the combined view)
CREATE VIEW user_timeline_base AS
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
FROM user_product_timeline_base
GROUP BY user_id, timestamp
ORDER BY user_id, timestamp;
