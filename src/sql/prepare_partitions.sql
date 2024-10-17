DROP TABLE IF EXISTS {name}_partitions;

CREATE TABLE {name}_partitions AS (
    select segment_id, road_type, geom
    from  {name}_segmented_ways
    );

ALTER TABLE {name}_partitions  ADD COLUMN if not exists part_id INT;
UPDATE {name}_partitions SET part_id = 1;
