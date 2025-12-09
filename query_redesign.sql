-- Refresh cumulative_cashflow without watermark

-- INSERT INTO cumulative_cashflows (
--     cashflow_id, user_id, product_id, timestamp,
--     units_held, net_investment, deposits, withdrawals, fees,
--     buy_units, sell_units, buy_cost, sell_proceeds
-- )
EXPLAIN ANALYZE
SELECT
    id,
    user_id,
    product_id,
    timestamp,
    -- 1. Simple Sums
    SUM(units_delta) OVER w AS units_held,
    SUM(execution_money) OVER w AS net_investment, -- Assuming execution_money = net flow
    
    -- 2. Conditional Sums (Python: if units > 0 ...)
    SUM(CASE WHEN units_delta > 0 THEN execution_money ELSE 0 END) OVER w AS deposits,
    SUM(CASE WHEN units_delta < 0 THEN -execution_money ELSE 0 END) OVER w AS withdrawals,
    SUM(fees) OVER w AS fees,
    
    -- 3. Trade Stats
    SUM(CASE WHEN units_delta > 0 THEN units_delta ELSE 0 END) OVER w AS buy_units,
    SUM(CASE WHEN units_delta < 0 THEN -units_delta ELSE 0 END) OVER w AS sell_units,
    SUM(CASE WHEN units_delta > 0 THEN execution_money ELSE 0 END) OVER w AS buy_cost,
    SUM(CASE WHEN units_delta < 0 THEN -execution_money ELSE 0 END) OVER w AS sell_proceeds

FROM cashflow
-- This defines the "Stream" logic
WINDOW w AS (
    PARTITION BY user_id, product_id  -- Reset counters for every new user/product pair
    ORDER BY timestamp, id            -- Process chronologically
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW -- Default behavior: Sum from start to now
);

-- Refresh user_product_cashflow_cache without watermark

EXPLAIN ANALYZE
WITH
    cashflow_intervals AS (
        SELECT cashflow_id, user_id, product_id,
               "timestamp" AS valid_from,
               LEAD("timestamp", 1, 'infinity'::timestamptz) OVER (
                   PARTITION BY user_id, product_id ORDER BY "timestamp", cashflow_id
               ) AS valid_to,
               units_held, net_investment, deposits, withdrawals, fees, buy_units, sell_units,
               buy_cost, sell_proceeds
        FROM cumulative_cashflow_cache
    )
SELECT ccf.user_id, ccf.product_id, ccf."timestamp", ccf.units_held, ccf.net_investment,
       ccf.deposits, ccf.withdrawals, ccf.fees, ccf.buy_units, ccf.sell_units, ccf.buy_cost,
       ccf.sell_proceeds, (ccf.units_held * pu.price) AS market_value
FROM cumulative_cashflow_cache ccf
    CROSS JOIN LATERAL (
        SELECT price 
        FROM price_update_15min pu
        WHERE pu.product_id = ccf.product_id AND pu.bucket <= ccf."timestamp"
        ORDER BY pu.bucket DESC 
        LIMIT 1
    ) pu
UNION ALL
SELECT ccf.user_id, ccf.product_id, pu.bucket, ccf.units_held, ccf.net_investment, ccf.deposits,
       ccf.withdrawals, ccf.fees, ccf.buy_units, ccf.sell_units, ccf.buy_cost, ccf.sell_proceeds,
       (ccf.units_held * pu.price) AS market_value
FROM cashflow_intervals ccf
    CROSS JOIN LATERAL (
        SELECT bucket, price
        FROM price_update_15min pu
        WHERE pu.product_id = ccf.product_id AND
              pu.bucket > ccf.valid_from AND pu.bucket < ccf.valid_to
    ) pu;


-- Refresh user_product_cashflow_cache without watermark
EXPLAIN ANALYZE
WITH user_product_intervals AS (
    SELECT 
           user_id, product_id,
           "timestamp" AS valid_from,
           -- The state is valid until the NEXT event for this same product
           LEAD("timestamp", 1, 'infinity'::timestamptz) OVER (
               PARTITION BY user_id, product_id 
               ORDER BY "timestamp"
           ) AS valid_to,
           net_investment, market_value, deposits, withdrawals, fees, buy_units, sell_units,
           buy_cost, sell_proceeds
    FROM user_product_timeline_cache_15min -- Result of your previous query
),
user_events AS (SELECT DISTINCT user_id, "timestamp" FROM user_product_timeline_cache_15min)
SELECT ue.user_id, ue."timestamp", SUM(upi.net_investment) AS net_investment,
       SUM(upi.market_value) AS market_value, SUM(upi.deposits) AS deposits,
       SUM(upi.withdrawals) AS withdrawals, SUM(upi.fees) AS fees, SUM(upi.buy_cost) AS buy_cost,
       SUM(upi.sell_proceeds) AS sell_proceeds,
       SUM(COALESCE(upi.buy_cost / NULLIF(upi.buy_units, 0), 0)) AS avg_buy_price,
       SUM(COALESCE(upi.sell_proceeds / NULLIF(upi.sell_units, 0), 0)) AS avg_sell_price
FROM user_events ue
    JOIN user_product_intervals upi
        ON ue.user_id = upi.user_id AND
           upi.valid_from <= ue."timestamp" AND upi.valid_to > ue."timestamp"
GROUP BY 1, 2
ORDER BY 1, 2
;

-- Refresh user_cashflow_cache without watermark (needs review)
EXPLAIN ANALYZE
WITH product_deltas AS (
    SELECT 
        user_id,
        "timestamp",
        -- 1. Track the raw components (Cost and Units) separately
        buy_cost       - LAG(buy_cost,       1, 0) OVER w AS d_buy_cost,
        buy_units      - LAG(buy_units,      1, 0) OVER w AS d_buy_units,
        
        sell_proceeds  - LAG(sell_proceeds,  1, 0) OVER w AS d_sell_proceeds,
        sell_units     - LAG(sell_units,     1, 0) OVER w AS d_sell_units,

        -- Standard additive fields still work with simple Deltas
        net_investment - LAG(net_investment, 1, 0) OVER w AS d_net_investment,
        market_value   - LAG(market_value,   1, 0) OVER w AS d_market_value,
        deposits       - LAG(deposits,       1, 0) OVER w AS d_deposits,
        withdrawals    - LAG(withdrawals,    1, 0) OVER w AS d_withdrawals,
        fees           - LAG(fees,           1, 0) OVER w AS d_fees

    FROM user_product_timeline_cache_15min
    WINDOW w AS (PARTITION BY user_id, product_id ORDER BY "timestamp")
),
daily_changes AS (
    -- 2. Collapse multiple product updates at the same timestamp
    SELECT 
        user_id,
        "timestamp",
        SUM(d_buy_cost)       as chg_buy_cost,
        SUM(d_buy_units)      as chg_buy_units,
        SUM(d_sell_proceeds)  as chg_sell_proceeds,
        SUM(d_sell_units)     as chg_sell_units,
        
        SUM(d_net_investment) as chg_net_invest,
        SUM(d_market_value)   as chg_market_val,
        SUM(d_deposits)       as chg_deposits,
        SUM(d_withdrawals)    as chg_withdrawals,
        SUM(d_fees)           as chg_fees
    FROM product_deltas
    GROUP BY user_id, "timestamp"
),
running_totals AS (
    -- 3. Reconstruct the Global State (Running Totals)
    SELECT 
        user_id,
        "timestamp",
        -- We accumulate the Numerators and Denominators separately here
        SUM(chg_buy_cost)      OVER w AS total_buy_cost,
        SUM(chg_buy_units)     OVER w AS total_buy_units,
        SUM(chg_sell_proceeds) OVER w AS total_sell_proceeds,
        SUM(chg_sell_units)    OVER w AS total_sell_units,
        
        SUM(chg_net_invest)    OVER w AS net_investment,
        SUM(chg_market_val)    OVER w AS market_value,
        SUM(chg_deposits)      OVER w AS deposits,
        SUM(chg_withdrawals)   OVER w AS withdrawals,
        SUM(chg_fees)          OVER w AS fees
    FROM daily_changes
    WINDOW w AS (PARTITION BY user_id ORDER BY "timestamp" ROWS UNBOUNDED PRECEDING)
)
-- 4. Final Calculation: Divide Total Cost / Total Units
SELECT 
    user_id, 
    "timestamp",
    net_investment,
    market_value,
    deposits,
    withdrawals,
    fees,
    total_buy_cost AS buy_cost,
    total_sell_proceeds AS sell_proceeds,
    -- The Weighted Averages
    COALESCE(total_buy_cost / NULLIF(total_buy_units, 0), 0) AS portfolio_avg_buy_price,
    COALESCE(total_sell_proceeds / NULLIF(total_sell_units, 0), 0) AS portfolio_avg_sell_price
FROM running_totals
ORDER BY user_id, "timestamp";
