-- create a grouping variable in segments
ALTER TABLE {table_name_segmented_ways} 
ADD COLUMN if not exists group_num INT;

--alter table {table_name_point_selection} drop column group_num;

-- Update the table to set the group number based on segment_number
UPDATE {table_name_segmented_ways}
SET group_num = segment_number / {segments_per_group};


drop table if exists {table_name_eval_groups} ;
drop table if exists {table_name_group_predictions} ;

---- new table: eval_groups with joined geometry
-- TODO: remove partition id
CREATE TABLE {table_name_eval_groups} AS
WITH GroupedSegments AS (
    SELECT
        {grouping_ids},
        -- id,
        -- part_id,
        -- group_num,
        road_type,
        ST_LineMerge(ST_Union(geom)) AS geometry
    FROM {table_name_segmented_ways}
    GROUP BY {grouping_ids}, road_type
    --GROUP BY elem_nr, id, part_id, group_num, road_type
)
SELECT *
FROM GroupedSegments;

CREATE INDEX {table_name_eval_groups}_idx ON {table_name_eval_groups} ({grouping_ids});

-- add group num to images

