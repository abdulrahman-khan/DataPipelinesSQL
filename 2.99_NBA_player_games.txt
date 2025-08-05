-- 1 deduped
WITH deduped AS(
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd., 
        ROW_NUMBER() OVER(PARTITION BY gd.game_id, team_id, player_id, g.game_date_est) as row_num
    FROM game_details gd 
        JOIN games g on gd.game_id = g.game_id
)
SELECT 
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' in comment),0)  0 as dim_did_not_play,
    COALESCE(POSITION('DND' in comment),0)  0 as dim_did_not_dress,
    COALESCE(POSITION('NWT' in comment),0)  0 as dim_not_with_team,
    CAST(SPLIT_PART(min, '', 1) AS REAL) + 
        CAST(SPLIT_PART(min, '', 2) AS REAL)60 as m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3a,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    TO as turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
FROM deduped
WHERE row_num = 1 
;

-- 2 DDL for user_devices_cumulated
DROP TABLE user_devices_cumulated;
CREATE TABLE user_devices_cumulated (
    user_id TEXT,
    device_activity_datelist JSON,
    date DATE,
    PRIMARY KEY (user_id, date)
);


-- -- 3 A cumulative query to generate device_activity_datelist from events
DO $$
DECLARE
    gen_series_date_d DATE;
BEGIN
    FOR gen_series_date_d IN SELECT generate_series(DATE '2022-12-31', DATE '2023-01-31', INTERVAL '1 day')
    LOOP
        WITH yesterday AS (
            SELECT 
                user_id, device_activity_datelist, date
            FROM user_devices_cumulated
            WHERE DATE(date) = DATE(gen_series_date_d - INTERVAL '1 day')
        ),  user_browser_history AS (
            SELECT
                e.user_id AS user_id,
                d.browser_type AS browser_type,
                date(e.event_time) AS date_active,
                array_agg(DISTINCT date(e.event_time) ORDER BY date(e.event_time)) AS activity_dates  -- date only here
            FROM events e 
            INNER JOIN devices d ON CAST(e.device_id AS TEXT) = CAST(d.device_id AS TEXT)
            WHERE DATE(e.event_time) = gen_series_date_d
            GROUP BY e.user_id, d.browser_type, date_active

        ), today AS (
            SELECT
                CAST(user_id AS TEXT),
                json_object_agg(browser_type, to_json(activity_dates)) AS device_activity_datelist,
                date_active
            FROM user_browser_history
            WHERE user_id IS NOT NULL
            GROUP BY user_id, date_active
        )
        INSERT INTO user_devices_cumulated
        SELECT
            CAST(COALESCE(t.user_id, y.user_id) AS TEXT) AS user_id,
            CASE
                WHEN y.device_activity_datelist IS NULL THEN t.device_activity_datelist -- first time visit
                WHEN t.device_activity_datelist IS NULL THEN y.device_activity_datelist -- didnt visit today, has visited in the past
                ELSE
                    (t.device_activity_datelistjsonb  y.device_activity_datelistjsonb)json            -- re-visit, add to array
            END AS device_activity_datelist,
            COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date                   -- adding 1 day
        FROM yesterday y
        FULL OUTER JOIN today t ON y.user_id = t.user_id
        ;
    END LOOP;
END $$;


-- -- 4 A datelist_int generation query. 
-- -- Convert the device_activity_datelist column into a datelist_int column
WITH users AS (
    SELECT user_id, device_activity_datelist, date
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31')
    -- and  user_id = '13211327936935700000'
), series AS (
    SELECT generate_seriesdate AS series_date
    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY')
), expanded AS (
    SELECT 
        u.user_id,
        u.date as date,
        key AS browser_type,
        value AS date_array_jsonb
    FROM users u
    CROSS JOIN LATERAL jsonb_each(u.device_activity_datelistjsonb)
), place_holder_ints AS(
    SELECT 
        CASE 
            WHEN date_array_jsonb @ to_jsonb(array[to_char(series_date, 'YYYY-MM-DD')])
                THEN CAST(POW(2, 32 - (date - DATE(series_date))) as BIGINT)
            ELSE 0
        END placeholder_int_value,
        
    FROM expanded
        CROSS JOIN series
)


SELECT 
    placeholder_int_value,
    user_id,
    CAST(CAST(SUM(placeholder_int_value)AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value)AS BIGINT) AS BIT(32)))  0 as dim_is_active
FROM place_holder_ints
where placeholder_int_value  0 
group by 1,2
order by 2 






-- 5 A DDL for hosts_cumulated table
-- a host_activity_datelist which logs to see which dates each host is experiencing any activity
DROP TABLE hosts_cumulated;
CREATE TABLE hosts_cumulated (
    host_name TEXT,
    host_activity_datelist DATE[],
    date DATE NOT NULL,
    PRIMARY KEY (host_name, date)
);

-- 6 The incremental query to generate host_activity_datelist
DO $$
DECLARE
    gen_series_date_d DATE;
BEGIN
    FOR gen_series_date_d IN SELECT generate_series(DATE '2022-12-31', DATE '2023-01-31', INTERVAL '1 day')
    LOOP
        WITH yesterday AS (
            SELECT 
                host_name,
                host_activity_datelist,
                date
            FROM hosts_cumulated
            WHERE date = gen_series_date_d - INTERVAL '1 day'
        ),
        today AS (
            SELECT
                host as host_name,
                DATE(event_time) AS today_date
            FROM events
            WHERE DATE(event_time) = gen_series_date_d
              AND host IS NOT NULL
            GROUP BY host, DATE(event_time)
        )
        INSERT INTO hosts_cumulated (host_name, host_activity_datelist, date)
        SELECT
            COALESCE(t.host_name, y.host_name) AS host_name,
            CASE
                WHEN y.host_activity_datelist IS NULL AND t.today_date IS NOT NULL THEN ARRAY[t.today_date]
                WHEN t.today_date IS NULL THEN y.host_activity_datelist
                ELSE ARRAY[t.today_date]  y.host_activity_datelist
            END AS host_activity_datelist,
            gen_series_date_d
        FROM yesterday y
        FULL OUTER JOIN today t ON y.host_name = t.host_name;
    END LOOP;
END $$;

-- 7 A monthly, reduced fact table DDL host_activity_reduced
DROP TABLE host_array_metrics;
CREATE TABLE host_array_metrics (
    host_name TEXT,
    month_start DATE,
    metric_name_hits TEXT,
    metric_array_hit REAL[],
    metric_name_unique_visitors TEXT,
    metric_array_unique_visitors REAL[],
    PRIMARY KEY(host_name, month_start, metric_name_hits, metric_name_unique_visitors)
);


-- 8 An incremental query that loads host_activity_reduced day-by-day
DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT generate_series(DATE '2023-01-01', DATE '2023-01-31', INTERVAL '1 day')
    LOOP
        WITH yesterday_array AS (
            SELECT  
            FROM host_array_metrics
            WHERE month_start = DATE('2023-01-01')
        ), daily_aggregate AS (
            SELECT
                host AS host_name,
                DATE(event_time) AS date,
                COUNT() AS num_hits,
                COUNT(DISTINCT user_idtext) AS num_unique_visitors
            FROM events
            WHERE host IS NOT NULL
              AND DATE(event_time) = d
            GROUP BY host, DATE(event_time)
        )
        INSERT INTO host_array_metrics
        SELECT 
            COALESCE(da.host_name, ya.host_name) as user_id,
            COALESCE(ya.month_start, DATE_TRUNC('month', da.date)) AS month_start,
            'site_hits' as metric_name_hits,
            CASE
                WHEN ya.metric_array_hit IS NOT NULL 
                    THEN ya.metric_array_hit  ARRAY[COALESCE(da.num_hits, 0)]
                WHEN ya.month_start IS NULL
                    THEN ARRAY[COALESCE(da.num_hits, 0)]
                WHEN ya.metric_array_hit IS NULL
                    THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)])  ARRAY[COALESCE(da.num_hits, 0)]
            END AS metric_array_hit,
            'unique_visitors' as metric_name_unique_visitors,
            CASE
                WHEN ya.metric_array_unique_visitors IS NOT NULL 
                    THEN ya.metric_array_unique_visitors  ARRAY[COALESCE(da.num_unique_visitors, 0)]
                WHEN ya.month_start IS NULL
                    THEN ARRAY[COALESCE(da.num_unique_visitors, 0)]
                WHEN ya.metric_array_unique_visitors IS NULL
                    THEN ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', date)), 0)])  ARRAY[COALESCE(da.num_unique_visitors, 0)]
            END AS metric_array_unique_visitors
        FROM daily_aggregate da
            FULL OUTER JOIN yesterday_array ya ON ya.host_name = da.host_name
        ON CONFLICT (host_name, month_start, metric_name_hits, metric_name_unique_visitors)
        DO UPDATE
        SET 
            metric_array_unique_visitors = EXCLUDED.metric_array_unique_visitors,
            metric_array_hit = EXCLUDED.metric_array_hit;
    END LOOP;
END $$;


