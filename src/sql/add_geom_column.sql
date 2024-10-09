ALTER TABLE {name}_img_metadata ADD COLUMN geom geometry(Point, 4326);

UPDATE {name}_img_metadata SET geom = ST_SetSRID(ST_MakePoint(lon, lat), 4326);

CREATE INDEX {name}_img_metadata_idx ON {name}_img_metadata USING GIST(geom);
