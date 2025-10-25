-- Table to track unique products and auto-assign product_id
CREATE TABLE product (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" TEXT UNIQUE NOT NULL
);

-- Table 1: Product Prices
CREATE TABLE product_price (
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    price NUMERIC(20, 6) NOT NULL CHECK (price > 0),

    PRIMARY KEY (product_id, timestamp),

    CONSTRAINT fk_product_price_product
    FOREIGN KEY (product_id)
    REFERENCES product (id)
    ON DELETE RESTRICT
);

-- Create indexes for efficient lookups
CREATE INDEX idx_product_prices_product_id_timestamp
ON product_price (product_id, timestamp DESC);

-- Table to track unique users and auto-assign user_id
CREATE TABLE "user" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "name" TEXT UNIQUE NOT NULL
);

-- Table 2: User Cash Flows with TWR tracking
CREATE TABLE user_cash_flow (
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL DEFAULT now(),
    units NUMERIC(20, 6) NOT NULL,  -- positive for buys, negative for sells
    -- money amount for this transaction (units * price)
    deposit NUMERIC(20, 6) NOT NULL,
    -- running total of units held after this transaction
    cumulative_units NUMERIC(20, 6),
    -- running total of net cash deposited (invested - withdrawn)
    cumulative_deposits NUMERIC(20, 6),
    period_return NUMERIC(20, 6),  -- return since last cash flow
    -- (1 + TWR) compounded, starts at 1.0
    cumulative_twr_factor NUMERIC(20, 6),

    PRIMARY KEY (user_id, product_id, timestamp),

    CONSTRAINT fk_user_cash_flow_user
    FOREIGN KEY (user_id)
    REFERENCES "user" (id)
    ON DELETE RESTRICT,

    CONSTRAINT fk_user_cash_flow_product
    FOREIGN KEY (product_id)
    REFERENCES product (id)
    ON DELETE RESTRICT
);

-- Create indexes for efficient lookups
CREATE INDEX idx_user_cash_flows_user_product
ON user_cash_flow (user_id, product_id, timestamp DESC);
