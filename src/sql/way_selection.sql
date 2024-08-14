-- drop table if it exists
drop table if exists {table_name_way_selection};
drop table if exists way_nodes_selection;
drop table if exists node_selection;
drop table if exists ways_selection;

CREATE TABLE node_selection AS
SELECT * FROM nodes WHERE ST_Within(
        geom,
         ST_SetSRID(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}), 4326)
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

-- this step takes some time when bbox is large (38 min for Dresden BBox)
create table {table_name_way_selection}  as
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
FROM ways_selection
where ways_selection.tags -> 'highway' != 'service'; -- exclude service ways

CREATE INDEX {table_name_way_selection}_idx ON {table_name_way_selection} USING GIST(geom);

drop table ways_selection;
drop table way_nodes_selection;
drop table node_selection;