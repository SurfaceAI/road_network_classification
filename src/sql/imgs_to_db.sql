drop table if exists {table_name_point_selection} ;

CREATE TABLE {table_name_point_selection} (
    img_id VARCHAR,
	tile_id VARCHAR,
    sequence_id VARCHAR,
    captured_at bigint,
    compass_angle double precision,
    is_pano bool,
    creator_id VARCHAR,
    lon double precision,
    lat double precision

);

COPY {table_name_point_selection} FROM '{img_metadata_path}' DELIMITER ',' CSV HEADER;

ALTER TABLE {table_name_point_selection} ADD COLUMN geom geometry(Point, 4326);

UPDATE {table_name_point_selection} SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);

CREATE INDEX {table_name_point_selection}_idx ON {table_name_point_selection} USING GIST(geom);
