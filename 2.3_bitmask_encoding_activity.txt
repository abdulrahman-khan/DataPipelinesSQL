
WITH users AS (
    SELECT *
    FROM users_cumulated
    where user_id = '13211327936935700000'
    WHERE date = DATE('2023-01-31')
), series AS (
    SELECT * 
    FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 DAY') as series_date
), place_holder_ints AS(
    SELECT 
        CASE 
            WHEN dates_active @> ARRAY[DATE(series_date)]
                THEN CAST(POW(2, 32 - (date - DATE(series_date))) as BIGINT)
            ELSE 0
        END placeholder_int_value,
        *
    FROM users
        CROSS JOIN series
    -- where user_id = '1244078867395324000'
)

select * from place_holder_ints where user_id = '13211327936935700000'

-- Select * from place_holder_ints

-- past 30 days
SELECT 
    user_id,
    CAST(CAST(SUM(placeholder_int_value)AS BIGINT) AS BIT(32)),
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value)AS BIGINT) AS BIT(32))) > 0 as dim_is_active,
    CAST('01100000000000100001010000110000' as BIT(32))
FROM place_holder_ints
group by 1

