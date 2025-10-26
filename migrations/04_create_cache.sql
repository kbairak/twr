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

    PRIMARY KEY (user_id, product_id, timestamp),
    CONSTRAINT fk_cache_user FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE,
    CONSTRAINT fk_cache_product FOREIGN KEY (product_id) REFERENCES product (id) ON DELETE CASCADE
);

-- Index for efficient queries
CREATE INDEX idx_cache_timestamp ON user_product_timeline_cache (timestamp DESC);
CREATE INDEX idx_cache_user_product ON user_product_timeline_cache (user_id, product_id, timestamp DESC);

-- Cache table for user-level timeline (aggregated across products)
CREATE TABLE user_timeline_cache (
    user_id UUID NOT NULL,
    "timestamp" TIMESTAMPTZ NOT NULL,
    total_net_deposits NUMERIC(20, 6),
    total_value NUMERIC(20, 6),
    value_weighted_twr NUMERIC(20, 6),
    PRIMARY KEY (user_id, timestamp),
    CONSTRAINT fk_user_timeline_cache_user FOREIGN KEY (user_id) REFERENCES "user" (id) ON DELETE CASCADE
);

-- Index for efficient queries
CREATE INDEX idx_user_timeline_cache_timestamp ON user_timeline_cache (timestamp DESC);
CREATE INDEX idx_user_timeline_cache_user ON user_timeline_cache (user_id, timestamp DESC);

-- Function to incrementally refresh the cache
CREATE OR REPLACE FUNCTION refresh_timeline_cache() RETURNS void AS $$
DECLARE
    v_watermark TIMESTAMPTZ;
    v_new_watermark TIMESTAMPTZ;
BEGIN
    -- Get current watermark from cache table (MAX timestamp already cached)
    SELECT MAX(timestamp) INTO v_watermark FROM user_product_timeline_cache;

    -- Determine new watermark (max timestamp from actual data)
    SELECT MAX(timestamp) INTO v_new_watermark FROM user_product_timeline_base;

    -- If there's no data, use current time
    IF v_new_watermark IS NULL THEN
        v_new_watermark := now();
    END IF;

    -- Insert new timeline data into cache (incremental)
    INSERT INTO user_product_timeline_cache
    SELECT user_id,
           product_id,
           timestamp,
           holdings,
           net_deposits,
           current_price,
           current_value,
           current_twr
    FROM user_product_timeline_base
    WHERE timestamp > COALESCE(v_watermark, '1970-01-01'::timestamptz) AND
          timestamp <= v_new_watermark
    ON CONFLICT (user_id, product_id, timestamp) DO NOTHING;

    -- Also cache user_timeline (aggregated view)
    INSERT INTO user_timeline_cache
    SELECT user_id,
           timestamp,
           total_net_deposits,
           total_value,
           value_weighted_twr
    FROM user_timeline_base
    WHERE timestamp > COALESCE(v_watermark, '1970-01-01'::timestamptz) AND
          timestamp <= v_new_watermark
    ON CONFLICT (user_id, timestamp) DO NOTHING;

    RAISE NOTICE 'Cache refreshed. New watermark: %', v_new_watermark;
END;
$$ LANGUAGE plpgsql;
