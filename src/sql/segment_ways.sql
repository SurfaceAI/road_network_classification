
CREATE INDEX IF NOT EXISTS {name}_way_selection_idx ON {name}_way_selection USING GIST(geom);

drop table if exists {name}_segmented_ways;

CREATE TABLE {name}_segmented_ways AS
SELECT 
   n.n AS segment_number,
   {additional_id_column}
   (original.id || '_' || n.n) AS segment_id,
    original.id as id,
    road_type,
    ST_LineSubstring(
        original.geom, 
        n.n::float / ceil(ST_Length(original.geom) / {segment_length}), 
        least((n.n + 1)::float / ceil(ST_Length(original.geom) / {segment_length}), 1)
    ) AS geom
FROM 
    {name}_way_selection AS original
CROSS JOIN 
    generate_series(0, ceil(ST_Length(original.geom) / {segment_length})::integer - 1) AS n(n)
WHERE 
    ST_Length(original.geom) > {min_road_length};

CREATE INDEX {name}_segmented_ways_idx ON {name}_segmented_ways USING GIST(geom);