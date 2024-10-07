drop table if exists {name}_img_metadata ;

CREATE TABLE {name}_img_metadata (
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

COPY {name}_img_metadata FROM '{img_metadata_path}' DELIMITER ',' CSV HEADER;

ALTER TABLE {name}_img_metadata ADD COLUMN geom geometry(Point, 4326);

UPDATE {name}_img_metadata SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);

CREATE INDEX {name}_img_metadata_idx ON {name}_img_metadata USING GIST(geom);
