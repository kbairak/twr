CREATE TABLE cumulative_cashflow_cache (
    -- Denormalized fields from cashflow for efficient querying
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,

    -- Monotonic totals - units (always increasing)
    buy_units NUMERIC(20, 6),         -- Σ(units_delta) for buys only
    sell_units NUMERIC(20, 6),        -- Σ(|units_delta|) for sells only

    -- Monotonic totals - execution_money (always increasing)
    buy_cost NUMERIC(20, 6),          -- Σ(execution_money) for buys
    sell_proceeds NUMERIC(20, 6),     -- Σ(|execution_money|) for sells

    -- Monotonic totals - cash flows (always increasing)
    deposits NUMERIC(20, 6),          -- Σ(user_money) for buys (what left bank)
    withdrawals NUMERIC(20, 6),       -- Σ(|user_money|) for sells (what entered bank)

    -- We can derive:
    --   - units = buy_units - sell_units
    --   - net_investment = deposits - withdrawals
    --   - fees = deposits - buy_cost + withdrawals - sell_proceeds

    CONSTRAINT unique_cashflow UNIQUE (user_id, product_id, "timestamp")
);

CREATE UNIQUE INDEX idx_cumulative_cashflow_cache_user_product
    ON cumulative_cashflow_cache(user_id, product_id, "timestamp" DESC);
CREATE INDEX idx_cumulative_cashflow_cache_product
    ON cumulative_cashflow_cache(product_id, "timestamp" DESC);
CREATE INDEX idx_cumulative_cashflow_cache_timestamp
    ON cumulative_cashflow_cache("timestamp" DESC);

CREATE VIEW cumulative_cashflow AS
    WITH
        seed_cf AS (
            SELECT DISTINCT ON (user_id, product_id)
                user_id, product_id, "timestamp", buy_units, sell_units, buy_cost, sell_proceeds,
                deposits, withdrawals
            FROM cumulative_cashflow_cache
            ORDER BY user_id, product_id, "timestamp" DESC
        ),
        fresh_cf AS (
            SELECT
                cf.user_id, cf.product_id, cf."timestamp",
                SUM(cf.units_delta) AS units_delta,
                (
                    SUM(cf.execution_price * cf.units_delta) / NULLIF(SUM(cf.units_delta), 0)
                ) AS execution_price,
                SUM(cf.user_money) AS user_money,
                -- units_delta * execution_price AS execution_money
                SUM(cf.units_delta) *
                    (SUM(cf.execution_price * cf.units_delta) / NULLIF(SUM(cf.units_delta), 0))
                    AS execution_money
            FROM cashflow cf
                LEFT OUTER JOIN seed_cf
                    ON cf.user_id = seed_cf.user_id AND cf.product_id = seed_cf.product_id
            WHERE seed_cf."timestamp" IS NULL OR cf."timestamp" > seed_cf."timestamp"
            GROUP BY cf.user_id, cf.product_id, cf."timestamp"
        )

    SELECT
        user_id, product_id, "timestamp", buy_units, sell_units, buy_cost, sell_proceeds, deposits,
        withdrawals
    FROM cumulative_cashflow_cache

    UNION ALL

    SELECT
        fresh_cf.user_id, fresh_cf.product_id, fresh_cf."timestamp",
        (
            COALESCE(seed_cf.buy_units, 0) +
            SUM(CASE WHEN fresh_cf.units_delta > 0 THEN fresh_cf.units_delta ELSE 0 END)
            OVER w
        ) AS buy_units,
        (
            COALESCE(seed_cf.sell_units, 0) +
            SUM(CASE WHEN fresh_cf.units_delta < 0 THEN -fresh_cf.units_delta ELSE 0 END)
            OVER w
        ) AS sell_units,
        (
            COALESCE(seed_cf.buy_cost, 0) +
            SUM(CASE WHEN fresh_cf.units_delta > 0 THEN fresh_cf.execution_money ELSE 0 END)
            OVER w
        ) AS buy_cost,
        (
            COALESCE(seed_cf.sell_proceeds, 0) +
            SUM(CASE WHEN fresh_cf.units_delta < 0 THEN -fresh_cf.execution_money ELSE 0 END)
            OVER w
        ) AS sell_proceeds,
        (
            COALESCE(seed_cf.deposits, 0) +
            SUM(CASE WHEN fresh_cf.units_delta > 0 THEN fresh_cf.user_money ELSE 0 END)
            OVER w
        ) AS deposits,
        (
            COALESCE(seed_cf.withdrawals, 0) +
            SUM(CASE WHEN fresh_cf.units_delta < 0 THEN -fresh_cf.user_money ELSE 0 END)
            OVER w
        ) AS withdrawals
    FROM fresh_cf
        LEFT OUTER JOIN seed_cf ON
            fresh_cf.user_id = seed_cf.user_id AND fresh_cf.product_id = seed_cf.product_id
    WINDOW w AS (
        PARTITION BY fresh_cf.user_id, fresh_cf.product_id
        ORDER BY fresh_cf."timestamp" ROWS
        BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    );

CREATE OR REPLACE FUNCTION refresh_cumulative_cashflow()
    RETURNS void AS $$
    DECLARE v_watermark TIMESTAMPTZ;
    BEGIN
        SELECT MAX("timestamp") INTO v_watermark
        FROM cumulative_cashflow_cache;

        INSERT INTO cumulative_cashflow_cache (
            user_id, product_id, "timestamp", buy_units, sell_units, buy_cost, sell_proceeds,
            deposits, withdrawals
        )
        SELECT
            user_id, product_id, "timestamp", buy_units, sell_units, buy_cost, sell_proceeds,
            deposits, withdrawals
        FROM cumulative_cashflow
        WHERE v_watermark IS NULL OR cumulative_cashflow."timestamp" > v_watermark;
    END;
    $$ LANGUAGE plpgsql;
