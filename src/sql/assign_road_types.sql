DROP TABLE IF EXISTS {name}_partitions;

CREATE TABLE {name}_partitions AS (
    select segment_id, road_type, geom
    from  {name}_segmented_ways
    );
ALTER TABLE {name}_partitions  ADD COLUMN if not exists part_id INT;
UPDATE {name}_partitions SET part_id = 1;

-- sidewalk right
with RoadTypeInfo as(
	select SEG.id, segment_id, sidewalk, sidewalk_left, sidewalk_right, cycleway_right, 
	cycleway_left,	cycleway_both,SEG.geom
	from {name}_segmented_ways as SEG
	join {name}_way_selection as WS
	on WS.id = SEG.id
)
INSERT INTO {name}_partitions (segment_id, part_id, road_type, geom)
SELECT segment_id, 2, 'footway', ST_OffsetCurve(geom, -5) 
FROM RoadTypeInfo
WHERE sidewalk = 'right' or sidewalk = 'both' or sidewalk_right = 'yes'
UNION ALL
-- sidewalk left
SELECT segment_id, 3, 'footway', ST_OffsetCurve(geom, 5) 
FROM RoadTypeInfo
WHERE sidewalk = 'left' or sidewalk = 'both' or sidewalk_left = 'yes'
UNION ALL
-- bike lane right
SELECT segment_id, 4, 'bike_lane', ST_OffsetCurve(geom, -2) 
FROM RoadTypeInfo
WHERE cycleway_right = 'lane' or cycleway_both = 'lane'
UNION ALL
-- bike lane left
SELECT segment_id, 5, 'bike_lane',ST_OffsetCurve(geom, 2) 
FROM RoadTypeInfo
WHERE cycleway_left = 'lane' or cycleway_both = 'lane'
UNION ALL
-- cycleway right
SELECT segment_id, 6, 'cycleway', ST_OffsetCurve(geom, -3.5) 
FROM RoadTypeInfo
WHERE cycleway_right = 'track' or cycleway_both = 'track'
UNION ALL
-- cycleway left
SELECT segment_id, 7, 'cycleway', ST_OffsetCurve(geom, 3.5) 
FROM RoadTypeInfo
WHERE cycleway_left = 'track' or cycleway_both = 'track';