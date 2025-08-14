-- SELECT 
--     dim_player_name, 
--     count(1) as num_games,
--     COUNT(CASE WHEN dim_not_with_team THEN 1 END),
--     CAST(COUNT(CASE WHEN dim_not_with_team THEN 1 END) AS REAL) / COUNT(1) AS bail
-- FROM fct_game_Detials
-- group by 1
-- order by 4 desc

-- DROP TABLE users_cumulated;
-- CREATE TABLE users_cumulated (
--     user_id TEXT,
--     -- list of dates in the past where the user was active
--     dates_active DATE[],
--     -- current dates for the user
--     date DATE,
--     PRIMARY KEY (user_id, date)    
-- );


-- INSERT INTO users_cumulated
-- WITH yesterday AS (w
--     SELECT *
--     FROM users_cumulated
--     WHERE date = ('2022-01-30')
-- ), today AS (
--     SELECT 
--         CAST(user_id AS TEXT) as user_id,
--         DATE(CAST(event_time AS TIMESTAMP)) as date_active
--     FROM events
--     WHERE 
--         DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
--         AND user_id IS NOT NULL
--     GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
-- )


-- -- all the users that are active today
-- SELECT 
--     COALESCE(t.user_id, y.user_id) as user_id,
--     CASE 
--         WHEN y.dates_active IS NULL
--             THEN ARRAY[t.date_active]
--         WHEN t.date_active IS NULL
--             THEN y.dates_active
--         ELSE ARRAY[t.date_active] || y.dates_active
--     END as dates_active,
--     COALESCE(t.date_active, y.date + INTERVAL '1 day') as date
-- FROM today t 
--     full outer join yesterday y on t.user_id = y.user_id
-- ;


DO $$
DECLARE
    d DATE;
BEGIN
    FOR d IN SELECT generate_series(DATE '2022-12-31', DATE '2023-01-31', INTERVAL '1 day')::DATE
    LOOP
        WITH yesterday AS (
            SELECT * 
            FROM users_cumulated
            WHERE date = d - INTERVAL '1 day'
        ),
        today AS (
            SELECT 
                CAST(host_id AS TEXT) AS user_id,
                d AS today_date
            FROM events
            WHERE event_time::DATE = d
              AND user_id IS NOT NULL
            GROUP BY user_id
        )
        INSERT INTO users_cumulated (user_id, dates_active, date)
        SELECT
            COALESCE(t.user_id, y.user_id) AS user_id,
            COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
            CASE 
                WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[] 
            END AS dates_active,
            d AS date
        FROM yesterday y
        FULL OUTER JOIN today t ON t.user_id = y.user_id;
    END LOOP;
END $$;

