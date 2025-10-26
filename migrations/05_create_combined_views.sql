-- Combined timeline view: Cached data UNION new delta
-- This is the main view users should query
-- It combines pre-computed cached data with freshly computed delta
CREATE VIEW user_product_timeline AS
WITH cached AS (
    SELECT *, TRUE as is_cached
    FROM user_product_timeline_cache
),
delta AS (
    -- Only compute events after the watermark (fast single-row lookup from watermark table)
    SELECT *, FALSE as is_cached
    FROM user_product_timeline_base
    WHERE timestamp > (SELECT last_cached_timestamp FROM cache_watermark WHERE id = 1)
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
    -- Only compute events after the watermark (fast single-row lookup from watermark table)
    SELECT *, FALSE as is_cached
    FROM user_timeline_base
    WHERE timestamp > (SELECT last_cached_timestamp FROM cache_watermark WHERE id = 1)
)
SELECT * FROM cached
UNION ALL
SELECT * FROM delta
ORDER BY user_id, timestamp;
