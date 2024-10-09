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
