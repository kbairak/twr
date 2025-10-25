-- Base timeline view: Portfolio state at each point in time when something changed
-- For each user-product pair, only includes relevant events:
-- - Cash flows for that specific user-product
-- - Price changes for that product (only after user's first cash flow)
-- This avoids creating redundant rows for products a user doesn't hold
-- This is the "base" view that computes everything - used by the cache system
CREATE VIEW user_product_timeline_base AS
WITH user_product_events AS (
    -- For each user-product pair, get only the relevant timestamps
    SELECT DISTINCT
        ucf.user_id,
        ucf.product_id,
        e.timestamp
    FROM (
        SELECT DISTINCT user_id, product_id
        FROM user_cash_flow
    ) ucf
    CROSS JOIN LATERAL (
        -- Cash flow timestamps for this user-product
        SELECT timestamp FROM user_cash_flow cf
        WHERE cf.user_id = ucf.user_id
          AND cf.product_id = ucf.product_id
        UNION
        -- Price change timestamps for this product, only after user's first cash flow
        SELECT pp.timestamp
        FROM product_price pp
        WHERE pp.product_id = ucf.product_id
          AND pp.timestamp >= (
              SELECT MIN(cf.timestamp)
              FROM user_cash_flow cf
              WHERE cf.user_id = ucf.user_id
                AND cf.product_id = ucf.product_id
          )
    ) e
),

user_product_state AS (
    -- For each relevant event, calculate the portfolio state at that moment
    SELECT
        upe.timestamp,
        upe.user_id,
        upe.product_id,
        -- Get the latest cash flow state up to this timestamp
        (
            SELECT cf.cumulative_units
            FROM user_cash_flow AS cf
            WHERE
                cf.user_id = upe.user_id
                AND cf.product_id = upe.product_id
                AND cf.timestamp <= upe.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS holdings,
        (
            SELECT cf.cumulative_deposits
            FROM user_cash_flow AS cf
            WHERE
                cf.user_id = upe.user_id
                AND cf.product_id = upe.product_id
                AND cf.timestamp <= upe.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS net_deposits,
        (
            SELECT cf.cumulative_twr_factor
            FROM user_cash_flow AS cf
            WHERE
                cf.user_id = upe.user_id
                AND cf.product_id = upe.product_id
                AND cf.timestamp <= upe.timestamp
            ORDER BY cf.timestamp DESC
            LIMIT 1
        ) AS twr_factor_at_last_flow,
        -- Get price at the time of last cash flow
        (
            SELECT pp.price
            FROM product_price AS pp
            WHERE
                pp.product_id = upe.product_id
                AND pp.timestamp <= (
                    SELECT cf.timestamp
                    FROM user_cash_flow AS cf
                    WHERE
                        cf.user_id = upe.user_id
                        AND cf.product_id = upe.product_id
                        AND cf.timestamp <= upe.timestamp
                    ORDER BY cf.timestamp DESC
                    LIMIT 1
                )
            ORDER BY pp.timestamp DESC
            LIMIT 1
        ) AS price_at_last_flow,
        -- Get current price at this event timestamp
        (
            SELECT pp.price
            FROM product_price AS pp
            WHERE
                pp.product_id = upe.product_id
                AND pp.timestamp <= upe.timestamp
            ORDER BY pp.timestamp DESC
            LIMIT 1
        ) AS current_price
    FROM user_product_events AS upe
)

SELECT
    ups.user_id,
    ups.product_id,
    ups.timestamp,
    ups.holdings,
    ups.net_deposits,
    ups.current_price,
    ups.holdings * ups.current_price AS current_value,
    -- Real-time TWR: compound TWR at last flow with price change since then
    CASE
        WHEN
            ups.price_at_last_flow > 0
            AND ups.current_price IS NOT NULL
            AND ups.twr_factor_at_last_flow IS NOT NULL
            THEN
                ups.twr_factor_at_last_flow
                * (ups.current_price / ups.price_at_last_flow)
                - 1
        ELSE
            0
    END AS current_twr
FROM user_product_state AS ups
WHERE
    ups.holdings IS NOT NULL
    AND ups.holdings != 0  -- Exclude times when user has no holdings
ORDER BY ups.user_id, ups.product_id, ups.timestamp;

-- Combined timeline view: Cached data UNION new delta
-- This is the main view users should query
-- It combines pre-computed cached data with freshly computed delta
CREATE VIEW user_product_timeline AS
WITH watermark AS (
    SELECT max_cached_timestamp FROM cache_watermark WHERE id = 1
),
cached AS (
    SELECT *, TRUE as is_cached
    FROM user_product_timeline_cache
),
delta AS (
    -- Only compute events after the watermark
    SELECT *, FALSE as is_cached
    FROM user_product_timeline_base
    WHERE timestamp > COALESCE((SELECT max_cached_timestamp FROM watermark), '1970-01-01'::timestamptz)
)
SELECT * FROM cached
UNION ALL
SELECT * FROM delta
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

-- Combined user timeline: Cached data UNION new delta
-- This is the main view users should query for user-level aggregated data
CREATE VIEW user_timeline AS
WITH watermark AS (
    SELECT max_cached_timestamp FROM cache_watermark WHERE id = 1
),
cached AS (
    SELECT *, TRUE as is_cached
    FROM user_timeline_cache
),
delta AS (
    -- Only compute events after the watermark
    SELECT *, FALSE as is_cached
    FROM user_timeline_base
    WHERE timestamp > COALESCE((SELECT max_cached_timestamp FROM watermark), '1970-01-01'::timestamptz)
)
SELECT * FROM cached
UNION ALL
SELECT * FROM delta
ORDER BY user_id, timestamp;
