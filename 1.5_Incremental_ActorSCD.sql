DROP TYPE scd_type CASCADE;
CREATE TYPE scd_type AS(
	quality_class quality_class,
	is_active BOOLEAN,
	start_season INTEGER,
	end_season INTEGER
);


-- Drop and re-create SCD type if needed
DROP TYPE IF EXISTS scd_type CASCADE;
CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
);

WITH last_year_scd AS (
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 2020
	  AND end_date = 2020
), historical_scd AS (
	SELECT *
	FROM actors_history_scd
	WHERE current_year = 2020
	  AND end_date < 2020
), this_year_data AS (
	SELECT *
	FROM actors
	WHERE year = 2021
), unchanged_records AS (
	SELECT 
		ts.actorid,
		COALESCE(ts.actor, '-') AS actor_name,
		ts.quality_class,
		ts.is_active,
		ls.start_date,
		ts.year AS end_date,
		ts.year AS current_year
	FROM this_year_data ts
	JOIN last_year_scd ls ON ts.actorid = ls.actorid
	WHERE 
		ts.quality_class = ls.quality_class
		AND ts.is_active = ls.is_active
), changed_records AS (
	SELECT 
		ts.actorid,
		COALESCE(ts.actor, '-') AS actor_name,
		UNNEST(ARRAY[
			ROW(
				ls.quality_class, 
				ls.is_active, 
				ls.start_date,
				ls.end_date
			)::scd_type,
			ROW(
				ts.quality_class, 
				ts.is_active, 
				ts.year,
				ts.year
			)::scd_type
		]) AS record
	FROM this_year_data ts
	LEFT JOIN last_year_scd ls ON ts.actorid = ls.actorid
	WHERE 
		ts.quality_class <> ls.quality_class
		OR ts.is_active <> ls.is_active
), unnested_changed_records AS (
	SELECT
		actorid,
		actor_name,
		(record).quality_class,
		(record).is_active,
		(record).start_date,
		(record).end_date,
		2021 AS current_year
	FROM changed_records
), new_records AS (
	SELECT 
		ts.actorid,
		COALESCE(ts.actor, '-') AS actor_name,
		ts.quality_class,
		ts.is_active,
		ts.year AS start_date,
		ts.year AS end_date,
		ts.year AS current_year
	FROM this_year_data ts
	LEFT JOIN last_year_scd ls ON ts.actorid = ls.actorid
	WHERE ls.actorid IS NULL
)
-- Final union of all updates
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;

