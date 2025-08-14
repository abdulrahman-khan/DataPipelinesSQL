WITH years AS (
  SELECT GENERATE_SERIES(1970, 2020) AS year
),
first_years AS (
  SELECT actorid, MIN(year) AS first_year
  FROM actor_films
  GROUP BY actorid
),
actor_years AS (
  SELECT
    f.actorid,
    y.year
  FROM first_years f
  JOIN years y ON y.year >= f.first_year
),
films_cumulative AS (
  SELECT
    ay.actorid,
    ay.year,
    ARRAY_REMOVE(ARRAY_AGG(
      CASE WHEN af.year <= ay.year THEN ROW(af.film, af.votes, af.rating, af.filmid)::films ELSE NULL END
    ), NULL) AS films
  FROM actor_years ay
  LEFT JOIN actor_films af ON ay.actorid = af.actorid
  GROUP BY ay.actorid, ay.year
),
quality_and_active AS (
  SELECT
    fc.actorid,
    fc.year,
    fc.films,
    CASE
      WHEN array_length(fc.films, 1) > 0 THEN
        CASE
          WHEN (SELECT AVG((f).rating) FROM UNNEST(fc.films) AS f) > 8 THEN 'star'::quality_class
          WHEN (SELECT AVG((f).rating) FROM UNNEST(fc.films) AS f) > 7 THEN 'good'::quality_class
          WHEN (SELECT AVG((f).rating) FROM UNNEST(fc.films) AS f) > 6 THEN 'average'::quality_class
          ELSE 'bad'::quality_class
        END
      ELSE 'bad'::quality_class
    END AS quality_class,
    EXISTS (
      SELECT 1 FROM actor_films af2 WHERE af2.actorid = fc.actorid AND af2.year = fc.year
    ) AS is_active
  FROM films_cumulative fc
)
INSERT INTO actors (actor, actorid, year, films, quality_class, is_active)
SELECT
  COALESCE(af.actor, '-') AS actor,
  qa.actorid,
  qa.year,
  qa.films,
  qa.quality_class,
  qa.is_active
FROM quality_and_active qa
LEFT JOIN (
  SELECT DISTINCT actorid, actor
  FROM actor_films
) af ON qa.actorid = af.actorid
ORDER BY qa.actorid, qa.year;
