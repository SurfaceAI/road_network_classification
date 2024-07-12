drop table if exists {table_name_point_selection};

CREATE TABLE {table_name_point_selection} AS 
  select *
  FROM (
  SELECT
  p.img_id, p.way_id, p.geom as point_geom, p.captured_at,
  -- ROW_NUMBER() OVER (PARTITION BY p.segment_id ORDER BY random()) AS rn -- random selection
  ROW_NUMBER() OVER (PARTITION BY p.segment_id ORDER BY p.captured_at DESC) AS rn -- select most current
  FROM  {table_name_snapped} p
) AS sampled
WHERE rn <= 10; -- select 3 points per segment

-- TODO: include heterogenity of users or segment ids