-- drop table if it exists
drop table if exists {table_name_way_selection};
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
FROM ways_selection;

CREATE INDEX {table_name_way_selection}_idx ON {table_name_way_selection} USING GIST(geom);


--alter table {table_name_way_selection} add column length_m double precision;
--update {table_name_way_selection} set length_m = st_length(geom);

drop table if exists segmented_ways;

CREATE TABLE segmented_ways AS
SELECT 
   (original.id || '_' || n.n) AS segment_id,
    original.id,
    ST_LineSubstring(
        original.geom, 
        n.n::float / ceil(ST_Length(original.geom) / {segment_length}), 
        least((n.n + 1)::float / ceil(ST_Length(original.geom) / {segment_length}), 1)
    ) AS geom
FROM 
    {table_name_way_selection} AS original
CROSS JOIN 
    generate_series(0, ceil(ST_Length(original.geom) / {segment_length})::integer - 1) AS n(n)
WHERE 
    ST_Length(original.geom) > 10;

CREATE INDEX segmented_ways_idx ON segmented_ways USING GIST(geom);


-- TODO: do by tile
drop table if exists {table_name_snapped};

 CREATE TABLE {table_name_snapped} AS (
  select p.id as img_id, n.way_id, n.segment_id,
  ST_ClosestPoint(n.geom, st_transform(p.geom, 3035)) AS geom,
  ST_Distance(n.geom, st_transform(p.geom, 3035)) AS dist
FROM
  {table_name} AS p
  CROSS JOIN LATERAL (
    SELECT
      l.geom, l.id as way_id,
      l.segment_id
    FROM
      segmented_ways AS l
    ORDER BY
      l.geom <-> st_transform(p.geom, 3035)
    LIMIT
      1
  ) AS n)
;

CREATE INDEX {table_name_snapped}_idx ON {table_name_snapped} USING GIST(geom);


alter table {table_name_snapped} 
add column lon int,
add column lat int;

update {table_name_snapped} 
set lon = ST_X(geom), 
lat = ST_Y(geom);

