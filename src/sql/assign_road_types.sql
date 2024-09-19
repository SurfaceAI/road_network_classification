ALTER TABLE {table_name_way_selection}  ADD COLUMN if not exists part_id INT;
UPDATE {table_name_way_selection} SET part_id = 1;

ALTER TABLE {table_name_segmented_ways}  ADD COLUMN if not exists part_id INT;
UPDATE {table_name_segmented_ways} SET part_id = 1;

-- sidewalk right
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 2, 'footway', ST_OffsetCurve(geom, -5) 
FROM {table_name_way_selection}
WHERE sidewalk = 'right' or sidewalk = 'both';

-- sidewalk left
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 3, 'footway', ST_OffsetCurve(geom, 5) 
FROM {table_name_way_selection}
WHERE sidewalk = 'left' or sidewalk = 'both';

-- bike lane right
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 4, 'bike_lane', ST_OffsetCurve(geom, -2) 
FROM {table_name_way_selection}
WHERE cycleway_right = 'lane' or cycleway_both = 'lane';

-- bike lane left
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 5, 'bike_lane',ST_OffsetCurve(geom, 2) 
FROM {table_name_way_selection}
WHERE cycleway_left = 'lane' or cycleway_both = 'lane';

-- cycleway right
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 6, 'cycleway', ST_OffsetCurve(geom, -3.5) 
FROM {table_name_way_selection}
WHERE cycleway_right = 'track' or cycleway_both = 'track';

-- cycleway left
INSERT INTO {table_name_way_selection} (id, part_id, road_type, geom)
SELECT id, 7, 'cycleway', ST_OffsetCurve(geom, 3.5) 
FROM {table_name_way_selection}
WHERE cycleway_left = 'track' or cycleway_both = 'track';
