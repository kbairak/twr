CREATE TABLE cumulative_cashflow_cache (
    cashflow_id UUID PRIMARY KEY REFERENCES cashflow(id),

    -- Denormalized fields from cashflow for efficient querying
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,

    -- Current state (net of all transactions up to this point)
    units_held NUMERIC(20, 6),        -- current holdings (buys - sells)
    net_investment NUMERIC(20, 6),    -- net cash (deposits - withdrawals)

    -- Monotonic totals - cash flows (always increasing)
    deposits NUMERIC(20, 6),          -- Σ(user_money) for buys (what left bank)
    withdrawals NUMERIC(20, 6),       -- Σ(|user_money|) for sells (what entered bank)
    fees NUMERIC(20, 6),              -- Σ(fees) for all transactions

    -- Monotonic totals - units (always increasing)
    buy_units NUMERIC(20, 6),         -- Σ(units_delta) for buys only
    sell_units NUMERIC(20, 6),        -- Σ(|units_delta|) for sells only

    -- Metric tracking (for unrealized/realized returns)
    buy_cost NUMERIC(20, 6),          -- Σ(units_delta × execution_price) for buys
    sell_proceeds NUMERIC(20, 6)      -- Σ(units_delta × execution_price) for sells
);

CREATE INDEX idx_cumulative_cashflow_cache_user_product ON cumulative_cashflow_cache(user_id, product_id, "timestamp" DESC);
CREATE INDEX idx_cumulative_cashflow_cache_product ON cumulative_cashflow_cache(product_id, "timestamp" DESC);
CREATE INDEX idx_cumulative_cashflow_cache_timestamp ON cumulative_cashflow_cache("timestamp" DESC);

-- =============================================================================
-- COMBINED VIEW: Cache + Delta Pattern
-- =============================================================================
-- This view intelligently combines:
--   1. Pre-computed cached rows (fast)
--   2. Fresh rows computed on-the-fly, seeded from last cached values (incremental)
--
-- When cache is empty: computes everything from scratch
-- When cache exists: only computes rows after the watermark, starting from cached values

CREATE VIEW cumulative_cashflow AS
    WITH
        seed_values AS (  -- last cached cumulative cashflow for each user-product
            SELECT DISTINCT ON (user_id, product_id)
                cashflow_id, user_id, product_id, "timestamp", units_held, net_investment, deposits,
                withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
            FROM cumulative_cashflow_cache
            ORDER BY user_id, product_id, "timestamp" DESC, cashflow_id
        ),
        fresh_cashflow AS (  -- Only cashflows after the cache
            SELECT cashflow.*
            FROM cashflow
                LEFT OUTER JOIN seed_values
                    ON seed_values.user_id = cashflow.user_id AND
                    seed_values.product_id = cashflow.product_id
            WHERE seed_values."timestamp" IS NULL OR cashflow."timestamp" > seed_values."timestamp"
        )
    SELECT cashflow_id, user_id, product_id, "timestamp", units_held, net_investment, deposits,
        withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
    FROM cumulative_cashflow_cache

    UNION ALL

    SELECT fresh_cashflow.id AS cashflow_id,
        fresh_cashflow.user_id,
        fresh_cashflow.product_id,
        fresh_cashflow."timestamp",
        COALESCE(seed_values.units_held, 0) + SUM(fresh_cashflow.units_delta) OVER w AS units_held,
        COALESCE(seed_values.net_investment, 0) + SUM(fresh_cashflow.user_money) OVER w AS net_investment,
        COALESCE(seed_values.deposits, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta > 0 THEN fresh_cashflow.user_money ELSE 0 END
        ) OVER w AS deposits,
        COALESCE(seed_values.withdrawals, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta < 0 THEN abs(fresh_cashflow.user_money) ELSE 0 END
        ) OVER w AS withdrawals,
        COALESCE(seed_values.fees, 0) + SUM(fresh_cashflow.fees) OVER w AS fees,
        COALESCE(seed_values.buy_units, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta > 0 THEN fresh_cashflow.units_delta ELSE 0 END
        ) OVER w AS buy_units,
        COALESCE(seed_values.sell_units, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta < 0 THEN abs(fresh_cashflow.units_delta) ELSE 0 END
        ) OVER w AS sell_units,
        COALESCE(seed_values.buy_cost, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta > 0 THEN fresh_cashflow.execution_money ELSE 0 END
        ) OVER w AS buy_cost,
        COALESCE(seed_values.sell_proceeds, 0) + SUM(
            CASE WHEN fresh_cashflow.units_delta < 0 THEN abs(fresh_cashflow.execution_money) ELSE 0 END
        ) OVER w AS sell_proceeds
    FROM fresh_cashflow
        LEFT OUTER JOIN seed_values
            ON fresh_cashflow.user_id = seed_values.user_id AND
            fresh_cashflow.product_id = seed_values.product_id
    WINDOW w AS (PARTITION BY fresh_cashflow.user_id, fresh_cashflow.product_id
                ORDER BY fresh_cashflow."timestamp"
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- Deletes cache entries for a specific user-product from a given timestamp onwards.
-- Used when out-of-order cashflows are inserted.
CREATE OR REPLACE FUNCTION invalidate_cumulative_cashflow_cache(
        p_user_id UUID, p_product_id UUID, p_from_timestamp TIMESTAMPTZ
    ) RETURNS void AS $$
    BEGIN
        DELETE FROM cumulative_cashflow_cache
        WHERE user_id = p_user_id AND product_id = p_product_id AND "timestamp" >= p_from_timestamp;
    END;
    $$ LANGUAGE plpgsql;

-- Repair cache for a specific user-product (repair gaps after invalidation)
CREATE OR REPLACE FUNCTION repair_cumulative_cashflow( p_user_id UUID, p_product_id UUID)
    RETURNS void AS $$
    DECLARE v_user_product_watermark TIMESTAMPTZ;
            v_overall_watermark TIMESTAMPTZ;
    BEGIN
        -- Find the overall cache watermark (largest cached timestamp across ALL user-products)
        SELECT MAX("timestamp") INTO v_overall_watermark
        FROM cumulative_cashflow_cache;

        -- If no cache exists at all (never been refreshed), nothing to repair - exit
        IF v_overall_watermark IS NULL THEN
            RETURN;
        END IF;

        -- Find the last cached timestamp for this specific user-product
        SELECT MAX("timestamp") INTO v_user_product_watermark
        FROM cumulative_cashflow_cache
        WHERE user_id = p_user_id AND product_id = p_product_id;

        -- Insert rows from (v_user_product_watermark, v_overall_watermark] to repair the gap
        -- If v_user_product_watermark is NULL, starts from beginning
        INSERT INTO cumulative_cashflow_cache (
            cashflow_id, user_id, product_id, "timestamp", units_held, net_investment,
            deposits, withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
        )
        SELECT cashflow_id, user_id, product_id, "timestamp", units_held, net_investment,
            deposits, withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
        FROM cumulative_cashflow
        WHERE user_id = p_user_id AND
            product_id = p_product_id AND
            (v_user_product_watermark IS NULL OR "timestamp" > v_user_product_watermark) AND
            "timestamp" <= v_overall_watermark;
    END;
    $$ LANGUAGE plpgsql;

-- Refresh cache for all user-products (move watermark forward)
-- IMPORTANT: After calling this function, run:
--   VACUUM ANALYZE cumulative_cashflow_cache;
-- This updates the visibility map for efficient index-only scans on the watermark
CREATE OR REPLACE FUNCTION refresh_cumulative_cashflow()
    RETURNS void AS $$
    DECLARE v_overall_watermark TIMESTAMPTZ;
    BEGIN
        -- Find the overall cache watermark (largest cached timestamp)
        SELECT MAX("timestamp") INTO v_overall_watermark
        FROM cumulative_cashflow_cache;

        -- Insert all rows after watermark (for all user-products)
        -- If watermark is NULL, fills everything from the beginning
        INSERT INTO cumulative_cashflow_cache (
            cashflow_id, user_id, product_id, "timestamp", units_held, net_investment,
            deposits, withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
        )
        SELECT cashflow_id, user_id, product_id, "timestamp", units_held, net_investment,
            deposits, withdrawals, fees, buy_units, sell_units, buy_cost, sell_proceeds
        FROM cumulative_cashflow
        WHERE v_overall_watermark IS NULL OR  -- No cache exists, fill everything
            cumulative_cashflow."timestamp" > v_overall_watermark;  -- After watermark
    END;
    $$ LANGUAGE plpgsql;

