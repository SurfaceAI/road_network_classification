drop table if exists {table_name} ;

CREATE TABLE {table_name} (
    id bigint,
	tile_id char(20),
    sequence_id char(50),
    captured_at bigint,
    compass_angle double precision,
    is_pano bool,
    creator_id char(20),
    lon double precision,
    lat double precision,
    class_prob double precision,
    surface_pred char(20)

);

COPY {table_name} FROM '{absolute_path}' DELIMITER ',' CSV HEADER;

ALTER TABLE {table_name} ADD COLUMN geom geometry(Point, 4326);

UPDATE {table_name} SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);