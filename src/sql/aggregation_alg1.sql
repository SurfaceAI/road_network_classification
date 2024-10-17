-- create a grouping variable in segments
ALTER TABLE {name}_segmented_ways
ADD COLUMN if not exists group_num INT;

-- Update the table to set the group number based on segment_number
UPDATE {name}_segmented_ways
SET group_num = {group_num};

drop table if exists {name}_eval_groups ;
drop table if exists {name}_group_predictions ;

---- new table: eval_groups with joined geometry
CREATE TABLE {name}_eval_groups AS
WITH GroupedSegments AS (
    SELECT
        {grouping_ids},
        road_type,
        ST_LineMerge(ST_Union(geom)) AS geometry
    FROM (
	    select {additional_ways_id_column}ways.id, ways.group_num, ways.segment_number, 
        part.part_id, part.road_type, part.geom
	    from {name}_segmented_ways ways
	    join {name}_partitions part
	    on ways.segment_id=part.segment_id
	    order by (ways.id, part.part_id, ways.segment_number)
    ) as partitions
    GROUP BY {grouping_ids}, road_type
)
SELECT *
FROM GroupedSegments;


CREATE INDEX {name}_eval_groups_idx ON {name}_eval_groups ({grouping_ids});

-- add group num to images

