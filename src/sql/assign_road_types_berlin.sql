-- alter road predictions to match target type

alter table {table_name_img_selection} add column if not exists fine_road_type_pred varchar;
UPDATE {table_name_img_selection}
SET fine_road_type_pred = road_type_pred;

update {table_name_img_selection} 
SET road_type_pred = CASE
    WHEN road_type_pred = 'bike_lane' THEN 'cycleway'
    ELSE road_type_pred
END;

-- no partitions for Berlin - therefore all ways have part_id = 1
ALTER TABLE {table_name_way_selection}  ADD COLUMN if not exists part_id INT;
UPDATE {table_name_way_selection} SET part_id = 1;

-- no partitions for Berlin - therefore all ways have part_id = 1
ALTER TABLE {table_name_segmented_ways}  ADD COLUMN if not exists part_id INT;
UPDATE {table_name_segmented_ways} SET part_id = 1;