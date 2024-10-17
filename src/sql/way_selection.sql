drop table if exists {name}_way_selection;
drop table if exists way_nodes_selection;
drop table if exists node_selection;
drop table if exists ways_selection;

CREATE TEMP TABLE node_selection AS
SELECT *, 
st_transform(geom, {crs}) AS geom_transformed
 FROM nodes WHERE ST_Within(
        geom,
         ST_SetSRID(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}), 4326)
    ); 
CREATE INDEX IF NOT EXISTS node_selection_idx ON node_selection (id);

CREATE TEMP TABLE way_nodes_selection as
select * from way_nodes 
JOIN node_selection ON way_nodes.node_id = node_selection.id;
--CREATE INDEX way_nodes_selection_idx ON way_nodes_selection USING GIST(geom_transformed);
CREATE INDEX IF NOT EXISTS way_nodes_selection_nodeidx ON way_nodes_selection (node_id);
--CREATE INDEX IF NOT EXISTS way_nodes_selection_wayidx ON way_nodes_selection (way_id);

CREATE TEMP TABLE ways_selection as
select distinct ways.* 
from ways
JOIN way_nodes_selection ON ways.id = way_nodes_selection.way_id
where ways.tags -> 'highway' not in ('construction', 'proposed', 'corridor', 'service'); -- exclude service ways


-- this step takes some time when bbox is large (38 min for Dresden BBox)
create table {name}_way_selection  as
select id, ways_selection.tags->'surface' as surface, 
ways_selection.tags ->'smoothness' as smoothness, 
ways_selection.tags -> 'highway' as highway,
ways_selection.tags -> 'cycleway' as cycleway,
ways_selection.tags -> 'cycleway:right' as cycleway_right,
ways_selection.tags -> 'cycleway:left' as cycleway_left,
ways_selection.tags -> 'cycleway:both' as cycleway_both,
ways_selection.tags -> 'sidewalk' as sidewalk,
ways_selection.tags -> 'sidewalk:right' as sidewalk_right,
ways_selection.tags -> 'sidewalk:left' as sidewalk_left,
ways_selection.tags -> 'foot' as foot,
(select ST_LineFromMultiPoint( ST_Collect(ns.geom_transformed order by wns.sequence_id))  AS geom
from node_selection as ns 
join way_nodes_selection as wns on ns.id=wns.node_id where wns.way_id=ways_selection.id ) 
FROM ways_selection;

CREATE INDEX {name}_way_selection_idx ON {name}_way_selection USING GIST(geom);

drop table ways_selection;
drop table way_nodes_selection;
drop table node_selection;

alter table {name}_way_selection add column if not exists road_type varchar;

UPDATE {name}_way_selection
SET road_type = CASE
    WHEN highway IN ('motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link', 
    'secondary', 'secondary_link', 'tertiary', 'tertiary_link', 'residential', 
    'living_street', 'service', 'road', 'unclassified') THEN 'road'
    WHEN highway IN ('track', 'path') THEN 'path'
    WHEN (highway = 'cycleway') and (cycleway = 'lane') THEN 'bike_lane'
    WHEN highway = 'cycleway' THEN 'cycleway'
    WHEN highway IN ('footway', 'pedestrian', 'steps') THEN 'footway'
END;