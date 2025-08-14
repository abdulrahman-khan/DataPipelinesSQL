DROP TABLE array_metrics;

CREATE TABLE array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY(user_id, month_start, metric_name)
);

WITH daily_aggregate AS (
    SELECT
        user_id,
        DATE(event_time) AS date,
        COUNT(1) as num_site_hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-04')
        AND user_id IS NOT NULL
    GROUP BY user_id, DATE(event_time)
), yesterday_array AS (
    SELECT * 
    FROM array_metrics
    WHERE month_start = DATE('2023-01-01')
)


INSERT INTO array_metrics
SELECT 
    COALESCE(da.user_id, ya.user_id) as user_id,
    COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
    'site_hits' as metric_name,
    CASE
        WHEN ya.metric_array IS NOT NULL 
            THEN ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.month_start IS NULL
            THEN ARRAY[COALESCE(da.num_site_hits, 0)]
        WHEN ya.metric_array IS NULL
            THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
    END AS metric_array
FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya ON ya.user_id = da.user_id
ON CONFLICT(user_id, month_start, metric_name) 
DO
    UPDATE SET metric_array = EXCLUDED.metric_array
;


select 
    cardinality(metric_array), count(1)
from array_metrics
group by 1
-- where user_id = 70132547320211180






