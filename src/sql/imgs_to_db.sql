drop table if exists {table_name} ;

CREATE TABLE {table_name} (
	tile_id VARCHAR,
    id VARCHAR,
    sequence_id VARCHAR,
    captured_at bigint,
    compass_angle double precision,
    is_pano bool,
    creator_id VARCHAR,
    lon double precision,
    lat double precision

);

COPY {table_name} FROM '{absolute_path}' DELIMITER ',' CSV HEADER;

ALTER TABLE {table_name} ADD COLUMN geom geometry(Point, 4326);

UPDATE {table_name} SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);

CREATE INDEX {table_name}_idx ON {table_name} USING GIST(geom);
