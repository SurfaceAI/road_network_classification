-- Create a temporary table to hold CSV data
CREATE TEMP TABLE temp_scenery_pred (
    img_id VARCHAR, 
    road_type VARCHAR
);

-- Import data from CSV file
COPY temp_scenery_pred(img_id,road_type)
FROM '{csv_path}'
WITH (FORMAT csv, HEADER true);

DROP TABLE IF EXISTS temp_pt_selection;

alter table {table_name_point_selection} 
drop column if exists road_type,
drop column if exists road_type_pred;

-- Update `points` table by setting `road_type` from `temp_road_type`
create TABLE temp_pt_selection AS
SELECT A.*, B.road_type, 
    CASE
        WHEN B.road_type LIKE '1_1_road%' OR B.road_type LIKE '1_4_path%' THEN 'road'
        WHEN B.road_type LIKE '1_3_pedestrian%' THEN 'pedestrian'
        WHEN B.road_type LIKE '1_2_bicycle__1_2_lane' THEN 'bike_lane'
        WHEN B.road_type LIKE '1_2_bicycle__1_2_cycleway' THEN 'cycleway'
        ELSE 'other'
    END AS road_type_pred
FROM  {table_name_point_selection} A
LEFT JOIN temp_scenery_pred B on A.img_id = B.img_id;

DROP TABLE  {table_name_point_selection};
ALTER TABLE temp_pt_selection RENAME TO  {table_name_point_selection};