CREATE TABLE IF NOT EXISTS cashflow (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    user_id         UUID NOT NULL,
    product_id      UUID NOT NULL,
    "timestamp"     TIMESTAMPTZ NOT NULL,

    units_delta     NUMERIC(20, 6),  -- positive for buys, negative for sells
    execution_price NUMERIC(20, 6),  -- price per unit
    user_money      NUMERIC(20, 6)   -- execution_money + fees (what left/entered bank)
    -- We can derive:
    --   * execution_money = units_delta × execution_price
    --   * fees = user_money - execution_money
    --   * user_price = user_money / units_delta
);

CREATE INDEX IF NOT EXISTS idx_cashflow_user_product_time ON cashflow(user_id, product_id, "timestamp" DESC, id);
CREATE INDEX IF NOT EXISTS idx_cashflow_timestamp         ON cashflow("timestamp" DESC, id);
