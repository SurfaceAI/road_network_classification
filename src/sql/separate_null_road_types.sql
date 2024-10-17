UPDATE {name}_partitions  
SET road_type = 'road'
WHERE road_type IS NULL;

-- insert all types for roads with no target road type
INSERT INTO {name}_partitions (segment_id, part_id, road_type, geom)
SELECT segment_id, 2, 'footway', ST_OffsetCurve(geom, -5) 
FROM {name}_segmented_ways
WHERE road_type IS NULL
UNION ALL
-- sidewalk left
SELECT segment_id, 3, 'footway', ST_OffsetCurve(geom, 5) 
FROM {name}_segmented_ways
WHERE road_type IS NULL
UNION ALL
-- bike lane right
SELECT segment_id, 4, 'bike_lane', ST_OffsetCurve(geom, -2) 
FROM {name}_segmented_ways
WHERE road_type IS NULL
UNION ALL
-- bike lane left
SELECT segment_id, 5, 'bike_lane',ST_OffsetCurve(geom, 2) 
FROM {name}_segmented_ways
WHERE road_type IS NULL
UNION ALL
-- cycleway right
SELECT segment_id, 6, 'cycleway', ST_OffsetCurve(geom, -3.5) 
FROM {name}_segmented_ways
WHERE road_type IS NULL
UNION ALL
-- cycleway left
SELECT segment_id, 7, 'cycleway', ST_OffsetCurve(geom, 3.5) 
FROM {name}_segmented_ways
WHERE road_type IS NULL;