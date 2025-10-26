-- Watermark table: tracks the latest timestamp cached (single row table)
-- This avoids expensive MAX(timestamp) queries on the cache tables
CREATE TABLE cache_watermark (
    id INT PRIMARY KEY DEFAULT 1,
    last_cached_timestamp TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'::TIMESTAMPTZ,
    CONSTRAINT single_row CHECK (id = 1)
);

-- Initialize with a single row
INSERT INTO cache_watermark (id, last_cached_timestamp) VALUES (1, '1970-01-01'::TIMESTAMPTZ);

-- Cache table: stores pre-computed timeline data for performance
-- Acts like a materialized view but supports incremental updates
CREATE TABLE user_product_timeline_cache (
    user_id UUID NOT NULL,
    product_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    holdings NUMERIC(20, 6),
    net_deposits NUMERIC(20, 6),
    current_price NUMERIC(20, 6),
    current_value NUMERIC(20, 6),
    current_twr NUMERIC(20, 6),

    PRIMARY KEY (user_id, product_id, timestamp)
);

-- Cache table for user-level timeline (aggregated across products)
CREATE TABLE user_timeline_cache (
    user_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    total_net_deposits NUMERIC(20, 6),
    total_value NUMERIC(20, 6),
    value_weighted_twr NUMERIC(20, 6),
    PRIMARY KEY (user_id, timestamp)
);

-- Function to incrementally refresh the cache
-- Optimized to compute user_product_timeline_base only once and use it for both caches
CREATE OR REPLACE FUNCTION refresh_timeline_cache() RETURNS void AS $$
DECLARE
    v_watermark TIMESTAMPTZ;
    v_new_watermark TIMESTAMPTZ;
BEGIN
    -- Get current watermark from dedicated watermark table (fast single-row lookup)
    SELECT last_cached_timestamp INTO v_watermark FROM cache_watermark WHERE id = 1;

    -- Determine new watermark (max timestamp from actual data)
    SELECT MAX(timestamp) INTO v_new_watermark FROM user_product_timeline_base;

    -- If there's no data, use current time
    IF v_new_watermark IS NULL THEN
        v_new_watermark := now();
    END IF;

    -- Compute product timeline data ONCE and use it for both caches
    -- This avoids computing user_product_timeline_base twice (once directly, once via user_timeline_base)
    WITH product_timeline_data AS (
        SELECT user_id,
               product_id,
               timestamp,
               holdings,
               net_deposits,
               current_price,
               current_value,
               current_twr
        FROM user_product_timeline_base
        WHERE timestamp > v_watermark
          AND timestamp <= v_new_watermark
    ),
    product_insert AS (
        -- Insert into product-level cache
        INSERT INTO user_product_timeline_cache
        SELECT * FROM product_timeline_data
        RETURNING 1
    )
    -- Insert into user-level cache by aggregating the product timeline data
    INSERT INTO user_timeline_cache
    SELECT
        user_id,
        timestamp,
        SUM(net_deposits) AS total_net_deposits,
        SUM(current_value) AS total_value,
        CASE
            WHEN SUM(current_value) > 0
            THEN SUM(current_twr * current_value) / SUM(current_value)
            ELSE 0
        END AS value_weighted_twr
    FROM product_timeline_data
    GROUP BY user_id, timestamp;

    -- Update watermark to new value
    UPDATE cache_watermark SET last_cached_timestamp = v_new_watermark WHERE id = 1;

    RAISE NOTICE 'Cache refreshed. New watermark: %', v_new_watermark;
END;
$$ LANGUAGE plpgsql;
