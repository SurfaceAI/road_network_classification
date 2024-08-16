-- TODO: set part_id

-- sidewalk right
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'footway', ST_OffsetCurve(geom, -5) 
FROM {table_name_way_selection}
WHERE sidewalk = 'right' or sidewalk = 'both';

-- sidewalk left
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'footway', ST_OffsetCurve(geom, 5) 
FROM {table_name_way_selection}
WHERE sidewalk = 'left' or sidewalk = 'both';

-- bike lane right
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'bike_lane', ST_OffsetCurve(geom, -2) 
FROM {table_name_way_selection}
WHERE cycleway_right = 'lane' or cycleway_both = 'lane';

-- bike lane left
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'bike_lane',ST_OffsetCurve(geom, 2) 
FROM {table_name_way_selection}
WHERE cycleway_left = 'lane' or cycleway_both = 'lane';

-- cycleway right
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'cycleway', ST_OffsetCurve(geom, -3.5) 
FROM {table_name_way_selection}
WHERE cycleway_right = 'track' or cycleway_both = 'track';

-- cycleway left
INSERT INTO {table_name_way_selection} (id, road_type, geom)
SELECT id, 'cycleway', ST_OffsetCurve(geom, 3.5) 
FROM {table_name_way_selection}
WHERE cycleway_left = 'track' or cycleway_both = 'track';
