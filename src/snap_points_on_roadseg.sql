-- drop table if it exists
drop table if exists sample_way_geometry;
drop table if exists way_nodes_selection;
drop table if exists node_selection;
drop table if exists ways_selection;


CREATE TABLE node_selection AS
SELECT * FROM nodes WHERE ST_Within(
        geom,
         ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326)
    ); 
   
CREATE INDEX node_selection_idx ON node_selection USING GIST(geom);

create table way_nodes_selection as
select * from way_nodes 
JOIN node_selection ON way_nodes.node_id = node_selection.id;

CREATE INDEX way_nodes_selection_idx ON way_nodes_selection USING GIST(geom);

create table ways_selection as
select distinct ways.* 
from ways
JOIN way_nodes_selection ON ways.id = way_nodes_selection.way_id;

create table sample_way_geometry  as
select id, ways_selection.tags->'surface' as surface, 
ways_selection.tags ->'smoothness' as smoothness, 
ways_selection.tags -> 'highway' as highway,
ways_selection.tags -> 'cycleway' as cycleway,
ways_selection.tags -> 'cycleway:right' as cycleway_right,
ways_selection.tags -> 'cycleway:left' as cycleway_left,
ways_selection.tags -> 'cycleway:both' as cycleway_both,
ways_selection.tags -> 'sidewalk' as sidewalk,
ways_selection.tags -> 'foot' as foot,
(select st_transform(ST_LineFromMultiPoint( ST_Collect(ns.geom order by wns.sequence_id)), 3035)  AS geom
from node_selection as ns join way_nodes_selection as wns on ns.id=wns.node_id where wns.way_id=ways_selection.id ) 
FROM ways_selection;


drop table if exists {table_name_snapped};

 CREATE TABLE {table_name_snapped} AS (
  select p.id as img_id, n.way_id,
  p.class_prob, p.surface_pred,
  ST_ClosestPoint(n.geom, st_transform(p.geom, 3035)) AS geom,
  ST_Distance(n.geom, st_transform(p.geom, 3035)) AS dist
FROM
  sample_aggregate AS p
  CROSS JOIN LATERAL (
    SELECT
       l.geom, l.id as way_id
    FROM
      sample_way_geometry AS l
    ORDER BY
      l.geom <-> st_transform(p.geom, 3035)
    LIMIT
      1
  ) AS n)
;

alter table {table_name_snapped} 
add column lon int,
add column lat int;

update {table_name_snapped} 
set lon = ST_X(geom), 
lat = ST_Y(geom);


-- add atribute to sample_way_geometry with majority vote
ALTER TABLE sample_way_geometry ADD COLUMN surface_pred char(20);

WITH SurfaceVotes AS (
    SELECT
        B.way_id,
        B.surface_pred,
        COUNT(*) AS vote_count
    FROM
        sample_aggregate_snapped B
    GROUP BY
        B.way_id, B.surface_pred
), RankedVotes AS (
    SELECT
        SV.way_id,
        SV.surface_pred,
        RANK() OVER (PARTITION BY SV.way_id ORDER BY SV.vote_count DESC) as rank
    FROM
        SurfaceVotes SV
)
UPDATE sample_way_geometry A
SET surface_pred = RV.surface_pred
FROM RankedVotes RV
WHERE A.id = RV.way_id AND RV.rank = 1;


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


