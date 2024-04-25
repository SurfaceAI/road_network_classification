
--- refine way geometries to represent all road types properly

-- modidfy way geometries

drop table if exists sample_way_geom_refined;

CREATE TABLE sample_way_geom_refined as 
(SELECT id, highway as road, geom from sample_way_geometry);

alter table sample_way_geom_refined add column with_offset bool;
update sample_way_geom_refined set with_offset=false;

--refine road values
UPDATE sample_way_geom_refined
	SET road = 'footway'
	WHERE road IN ('pedestrian');

UPDATE sample_way_geom_refined
SET road = 'roadway'
WHERE road IN ('motorway', 'motorway_link',
				'trunk', 'trunk_link,'
				'primary', 'primary_link', 
				'secondary', 'secondary_link', 
				'tertiary', 'tertiary_link',
				'unclassified', 'residential', 'living_street');
			
UPDATE sample_way_geom_refined
SET road = 'path'
WHERE road IN ('track');

-- ---- is there a cycleway? ----
-- cycleway / cycleway:right / cycleway:left / cycleway:both
-- *:lane, *:track --> use same geometry with cycleway
-- duplicate geometry (if left and right, then tripple), for "left", reverse order

INSERT INTO sample_way_geom_refined
SELECT id, 'cylceway' as road, geom, true as with_offset
FROM sample_way_geometry
WHERE 
 (cycleway in ('track', 'lane'))  
 or (cycleway_right IN ('track', 'lane')) 
 or (cycleway_both IN ('track', 'lane'));

INSERT INTO sample_way_geom_refined
SELECT id, 'cylceway' as road, st_reverse(geom), true as with_offset
FROM sample_way_geometry
WHERE 
 (cycleway in ('track', 'lane'))  
 or (cycleway_left IN ('track', 'lane')) 
 or (cycleway_both IN ('track', 'lane'));


-- same with sidewalk --

INSERT INTO sample_way_geom_refined
SELECT id, 'footway' as road, geom, true as with_offset
FROM sample_way_geometry
WHERE 
 (sidewalk in ('right', 'both'));

INSERT INTO sample_way_geom_refined
SELECT id, 'footway' as road, st_reverse(geom), true as with_offset
FROM sample_way_geometry
WHERE 
 (sidewalk in ('left', 'both'));


