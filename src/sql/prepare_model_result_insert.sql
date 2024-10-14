drop table if exists temp_classification_updates;
create TABLE  temp_classification_updates (
	img_id VARCHAR,
    road_type_pred VARCHAR,
	road_type_prob double precision,
    type_pred VARCHAR,
    type_class_prob double precision,
    quality_pred double precision);