CREATE TEMP TABLE temp_transformed AS
SELECT
  img_id,
  tile_id,
  sequence_id,
  captured_at,
  st_transform(geom, {crs}) AS geom
FROM
  {name};
 
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
      seg.way_id,
      seg.segment_id,
      p.geom,
      to_timestamp(p.captured_at / 1000) as captured_at_timestamp,
      ST_ClosestPoint(seg.geom, p.geom) AS geom_snapped,
      ST_Distance(seg.geom, p.geom) AS dist
    FROM
      temp_transformed AS p
      CROSS JOIN LATERAL (
        SELECT
          l.geom,
          l.id AS way_id,
          l.segment_id
        FROM
          {name}_segmented_ways AS l
        ORDER BY
          l.geom <-> p.geom -- order by distance
        LIMIT 1
      ) AS seg
  ) AS subquery
  WHERE dist <= {dist_from_road} -- only consider a road a match if within x meters
);
-- img within close proximity of how many roads?
ALTER TABLE temp_table ADD COLUMN num_closeby_ways INT;

CREATE TEMP TABLE CloseByRoads AS
  SELECT img.img_id, COUNT(*) AS num_closeby_ways
  FROM temp_table AS img
  JOIN {name}_way_selection AS ws
  ON ST_DWithin(img.geom, ws.geom, 10)
  GROUP BY img.img_id;

CREATE INDEX CloseByRoads_idx ON CloseByRoads (img_id);

UPDATE temp_table
SET num_closeby_ways = (
  SELECT num_closeby_ways
  FROM CloseByRoads
  WHERE temp_table.img_id = CloseByRoads.img_id
);

DROP TABLE  {name};
ALTER TABLE temp_table RENAME TO  {name};

DROP TABLE  temp_transformed;

CREATE INDEX {name}_idx ON {name} USING GIST(geom);