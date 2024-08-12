--DELETE FROM {name_way_selection}
--WHERE highway IS NULL;

alter table test_way_selection add column if not exists road_type varchar;

-- remove highways of type: construction, proposed, corridor
DELETE FROM {name_way_selection}
WHERE highway in ('construction', 'proposed', 'corridor');

UPDATE {name_way_selection}
SET road_type = CASE
    WHEN highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'unclassified', 'residential', 'living_street', 'service', 'track', 'road', 'path') THEN 'road/path'
    WHEN (highway = 'cycleway') and (cycleway = 'lane') THEN 'bike_lane'
    WHEN highway = 'cycleway' THEN 'cycleway'
    WHEN highway IN ('footway', 'pedestrian', 'steps') THEN 'footway'
END;

-- sidewalk right
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'footway', ST_OffsetCurve(geom, -5) 
FROM {name_way_selection}
WHERE sidewalk = 'right' or sidewalk = 'both';

-- sidewalk left
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'footway', ST_OffsetCurve(geom, 5) 
FROM {name_way_selection}
WHERE sidewalk = 'left' or sidewalk = 'both';

-- bike lane right
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'bike_lane', ST_OffsetCurve(geom, -2) 
FROM {name_way_selection}
WHERE cycleway_right = 'lane' or cycleway_both = 'lane';

-- bike lane left
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'bike_lane',ST_OffsetCurve(geom, 2) 
FROM {name_way_selection}
WHERE cycleway_left = 'lane' or cycleway_both = 'lane';

-- cycleway right
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'cycleway', ST_OffsetCurve(geom, -3.5) 
FROM {name_way_selection}
WHERE cycleway_right = 'track' or cycleway_both = 'track';

-- cycleway left
INSERT INTO {name_way_selection} (id, road_type, geom)
SELECT id, 'cycleway', ST_OffsetCurve(geom, 3.5) 
FROM {name_way_selection}
WHERE cycleway_left = 'track' or cycleway_both = 'track';



-- merge with prediction
-- only keep rows where road type and pred type match
-- road_type == pred_type