DROP TABLE actors;
DROP TYPE quality_class CASCADE;
DROP TYPE films CASCADE;

CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);
CREATE TYPE quality_class AS ENUM('star', 'good', 'average', 'bad');
CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	films films[],
	year INTEGER,
	quality_class quality_class,
	is_active BOOLEAN DEFAULT FALSE
);
