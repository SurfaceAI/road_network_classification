drop table if exists {table_name_point_selection};

CREATE TABLE {table_name_point_selection} AS 
  select *
  FROM (
  SELECT
  p.img_id, p.way_id, p.segment_id, p.geom as point_geom, p.captured_at,
  -- ROW_NUMBER() OVER (PARTITION BY p.segment_id ORDER BY random()) AS rn -- random selection
  ROW_NUMBER() OVER (PARTITION BY p.segment_id ORDER BY p.captured_at DESC) AS rn -- select most current
  FROM  {table_name} p
) AS sampled
WHERE rn <= 10; -- select n points per segment
