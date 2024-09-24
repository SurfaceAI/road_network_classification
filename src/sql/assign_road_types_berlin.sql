-- alter road predictions to match target type
DROP TABLE IF EXISTS {name}_partitions;

CREATE TABLE {name}_partitions AS (
    select segment_id, road_type, geom
    from  {name}_segmented_ways
    );

ALTER TABLE {name}_partitions  ADD COLUMN if not exists part_id INT;
UPDATE {name}_partitions SET part_id = 1;


ALTER TABLE {table_name_img_selection} add column if not exists fine_road_type_pred varchar;
UPDATE {table_name_img_selection}
SET fine_road_type_pred = road_type_pred;

update {table_name_img_selection} 
SET road_type_pred = CASE
    WHEN road_type_pred = 'bike_lane' THEN 'cycleway'
    ELSE road_type_pred
END;
