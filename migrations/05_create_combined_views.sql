-- Combined timeline view: Cached data UNION new delta
-- This is the main view users should query
-- It combines pre-computed cached data with freshly computed delta
CREATE VIEW user_product_timeline AS
WITH cached AS (
    SELECT *, TRUE as is_cached
    FROM user_product_timeline_cache
),
delta AS (
    -- Only compute events after the watermark (MAX timestamp in cache)
    SELECT *, FALSE as is_cached
    FROM user_product_timeline_base
    WHERE timestamp > COALESCE((SELECT MAX(timestamp) FROM user_product_timeline_cache), '1970-01-01'::timestamptz)
)
SELECT * FROM cached
UNION ALL
SELECT * FROM delta
ORDER BY user_id, product_id, timestamp;

-- Combined user timeline: Cached data UNION new delta
-- This is the main view users should query for user-level aggregated data
CREATE VIEW user_timeline AS
WITH cached AS (
    SELECT *, TRUE as is_cached
    FROM user_timeline_cache
),
delta AS (
    -- Only compute events after the watermark (MAX timestamp in cache)
    SELECT *, FALSE as is_cached
    FROM user_timeline_base
    WHERE timestamp > COALESCE((SELECT MAX(timestamp) FROM user_timeline_cache), '1970-01-01'::timestamptz)
)
SELECT * FROM cached
UNION ALL
SELECT * FROM delta
ORDER BY user_id, timestamp;
