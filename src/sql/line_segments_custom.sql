-- drop table if it exists
drop table if exists {table_name_way_selection};

CREATE TABLE {table_name_way_selection} AS
SELECT * FROM {edge_table_name} WHERE ST_Within(
        geom,
        st_transform(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326), 25833)
    ); 


--alter table {table_name_way_selection} RENAME COLUMN {old_way_id_name} to id;
   
CREATE INDEX {table_name_way_selection}_idx ON {table_name_way_selection} USING GIST(geom);

drop table if exists segmented_ways;

CREATE TABLE segmented_ways AS
SELECT 
   n.n AS segment_number,
   (original.id || '_' || n.n) AS segment_id,
    original.id as id,
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