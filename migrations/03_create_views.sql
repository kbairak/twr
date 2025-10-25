-- Timeline view: Portfolio state at each point in time when something changed
-- Combines all events (cash flows and price changes) into a timeline
CREATE VIEW user_portfolio_timeline AS
WITH events AS (
    -- Get all timestamps where something happened (cash flow or price change)
    SELECT DISTINCT timestamp
    FROM (
        SELECT timestamp FROM user_cash_flow
        UNION
        SELECT timestamp FROM product_price
    ) all_timestamps
),
user_product_state AS (
    -- For each event timestamp and each user-product combination,
    -- calculate the portfolio state at that moment
    SELECT
        e.timestamp,
        ucf.user_id,
        ucf.product_id,
        -- Get the latest cash flow state up to this timestamp
        (
            SELECT cumulative_units
            FROM user_cash_flow cf
            WHERE cf.user_id = ucf.user_id
              AND cf.product_id = ucf.product_id
              AND cf.timestamp <= e.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS holdings,
        (
            SELECT cumulative_deposits
            FROM user_cash_flow cf
            WHERE cf.user_id = ucf.user_id
              AND cf.product_id = ucf.product_id
              AND cf.timestamp <= e.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS net_deposits,
        (
            SELECT cumulative_twr_factor
            FROM user_cash_flow cf
            WHERE cf.user_id = ucf.user_id
              AND cf.product_id = ucf.product_id
              AND cf.timestamp <= e.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS twr_factor_at_last_flow,
        -- Get price at the time of last cash flow
        (
            SELECT pp.price
            FROM product_price pp
            WHERE pp.product_id = ucf.product_id
              AND pp.timestamp <= (
                  SELECT cf.timestamp
                  FROM user_cash_flow cf
                  WHERE cf.user_id = ucf.user_id
                    AND cf.product_id = ucf.product_id
                    AND cf.timestamp <= e.timestamp
                  ORDER BY cf.timestamp DESC
                  LIMIT 1
              )
            ORDER BY pp.timestamp DESC
            LIMIT 1
        ) AS price_at_last_flow,
        -- Get current price at this event timestamp
        (
            SELECT price
            FROM product_price pp
            WHERE pp.product_id = ucf.product_id
              AND pp.timestamp <= e.timestamp
            ORDER BY pp.timestamp DESC
            LIMIT 1
        ) AS current_price
    FROM events e
    CROSS JOIN (
        SELECT DISTINCT user_id, product_id
        FROM user_cash_flow
    ) ucf
    WHERE EXISTS (
        -- Only include if user has holdings at this time
        SELECT 1
        FROM user_cash_flow cf
        WHERE cf.user_id = ucf.user_id
          AND cf.product_id = ucf.product_id
          AND cf.timestamp <= e.timestamp
    )
)
SELECT
    u.name AS user_name,
    p.name AS product_name,
    ups.timestamp,
    ups.holdings,
    ups.net_deposits,
    ups.current_price,
    ups.holdings * ups.current_price AS current_value,
    -- Real-time TWR: compound TWR at last flow with price change since then
    CASE
        WHEN ups.price_at_last_flow > 0 AND ups.current_price IS NOT NULL AND ups.twr_factor_at_last_flow IS NOT NULL THEN
            ups.twr_factor_at_last_flow * (ups.current_price / ups.price_at_last_flow) - 1
        ELSE
            0
    END AS current_twr
FROM user_product_state ups
JOIN app_user u ON ups.user_id = u.id
JOIN product p ON ups.product_id = p.id
WHERE ups.holdings IS NOT NULL
  AND ups.holdings != 0  -- Exclude times when user has no holdings
ORDER BY u.name, p.name, ups.timestamp;

-- Product-level timeline: Full timeline for each user-product
-- Shows portfolio state at every event (cash flow or price change)
CREATE VIEW user_product_timeline AS
SELECT
    user_name,
    product_name,
    timestamp,
    holdings,
    net_deposits,
    current_price,
    current_value,
    current_twr
FROM user_portfolio_timeline
ORDER BY user_name, product_name, timestamp;

-- User-level timeline: Aggregated portfolio value over time
CREATE VIEW user_timeline AS
SELECT
    user_name,
    timestamp,
    SUM(net_deposits) AS total_net_deposits,
    SUM(current_value) AS total_value,
    -- Value-weighted TWR across all products
    CASE
        WHEN SUM(current_value) > 0 THEN
            SUM(current_twr * current_value) / SUM(current_value)
        ELSE
            0
    END AS value_weighted_twr
FROM user_portfolio_timeline
GROUP BY user_name, timestamp
ORDER BY user_name, timestamp;
