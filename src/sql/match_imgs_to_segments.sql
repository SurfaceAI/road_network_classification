CREATE TEMP TABLE temp_transformed AS
SELECT
  id AS img_id,
  tile_id,
  sequence_id,
  captured_at,
  st_transform(geom, {crs}) AS geom
FROM
  {table_name};
 
CREATE INDEX temp_transformed_idx ON temp_transformed USING GIST(geom);


CREATE TABLE temp_table AS (
  SELECT
  *
  FROM (
    SELECT
      img_id,
      p.tile_id,
      p.sequence_id,
      p.captured_at,
      n.way_id,
      n.segment_id,
      p.geom,
      to_timestamp(p.captured_at / 1000) as captured_at_timestamp,
      ST_ClosestPoint(n.geom, p.geom) AS geom_snapped,
      ST_Distance(n.geom, p.geom) AS dist
    FROM
      temp_transformed AS p
      CROSS JOIN LATERAL (
        SELECT
          l.geom,
          l.id AS way_id,
          l.segment_id
        FROM
          {table_name_segmented_ways} AS l
        ORDER BY
          l.geom <-> p.geom -- order by distance
        LIMIT 1
      ) AS n
  ) AS subquery
  WHERE dist <= 10 -- only consider a road a match if within 10 meters
);
DROP TABLE  {table_name};
ALTER TABLE temp_table RENAME TO  {table_name};

DROP TABLE  temp_transformed;

CREATE INDEX {table_name}_idx ON {table_name} USING GIST(geom);