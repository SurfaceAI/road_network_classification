CREATE TABLE temp_table AS (
  SELECT
  *
  FROM (
    SELECT
      p.id AS img_id,
      p.tile_id,
      p.sequence_id,
      p.captured_at,
      n.way_id,
      n.segment_id,
      p.geom,
      to_timestamp(p.captured_at / 1000) as captured_at_timestamp,
      ST_ClosestPoint(n.geom, st_transform(p.geom, 25833)) AS geom_snapped,
      ST_Distance(n.geom, st_transform(p.geom, 25833)) AS dist
    FROM
      {table_name} AS p
      CROSS JOIN LATERAL (
        SELECT
          l.geom,
          l.id AS way_id,
          l.segment_id
        FROM
          segmented_ways AS l
        ORDER BY
          l.geom <-> st_transform(p.geom, 25833) -- order by distance
        LIMIT 1
      ) AS n
  ) AS subquery
  WHERE dist <= 10 -- only consider a road a match if within 10 meters
);
DROP TABLE  {table_name};
ALTER TABLE temp_table RENAME TO  {table_name};
