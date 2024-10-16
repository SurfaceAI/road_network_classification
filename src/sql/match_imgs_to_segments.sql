CREATE TEMP TABLE temp_transformed AS
SELECT
  img_id,
  sequence_id,
  captured_at,
  st_transform(geom, {crs}) AS geom
FROM
  {name}_img_metadata;
 
CREATE INDEX temp_transformed_idx ON temp_transformed USING GIST(geom);


CREATE TABLE temp_table AS (
  SELECT
  *
  FROM (
    SELECT
      img_id,
      p.sequence_id,
      p.captured_at,
      seg.way_id,
      seg.segment_id,
      p.geom,
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

-- TODO: compute distance between points and roads only once
CREATE TEMP TABLE CloseByRoads AS
  SELECT img.img_id, COUNT(*) AS num_closeby_ways
  FROM temp_table AS img
  JOIN {name}_way_selection AS ws
  ON ST_DWithin(img.geom, ws.geom, {dist_from_road})
  GROUP BY img.img_id;

CREATE INDEX CloseByRoads_idx ON CloseByRoads (img_id);

UPDATE temp_table
SET num_closeby_ways = (
  SELECT num_closeby_ways
  FROM CloseByRoads
  WHERE temp_table.img_id = CloseByRoads.img_id
);

DROP TABLE  {name}_img_metadata;
ALTER TABLE temp_table RENAME TO  {name}_img_metadata;

DROP TABLE  temp_transformed;

CREATE INDEX {name}_img_metadata_idx ON {name}_img_metadata USING GIST(geom);