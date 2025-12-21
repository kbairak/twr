CREATE TABLE cumulative_cashflow_cache (
    cashflow_id UUID PRIMARY KEY REFERENCES cashflow(id),

    -- Denormalized fields from cashflow for efficient querying
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,

    -- Current state (net of all transactions up to this point)
    units NUMERIC(20, 6),        -- current holdings (buys - sells)
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
