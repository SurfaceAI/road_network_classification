DROP INDEX IF EXISTS {name}_img_classifications_idx;

CREATE TABLE  IF NOT EXISTS {name}_img_classifications (
	img_id VARCHAR,
    road_type_pred VARCHAR,
	road_type_prob double precision,
    type_pred VARCHAR,
    type_class_prob double precision,
    quality_pred double precision);