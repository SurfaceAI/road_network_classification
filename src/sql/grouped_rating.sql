-- create a grouping variable in segments
ALTER TABLE segmented_ways ADD COLUMN if not exists group_num INT;

--alter table berlin_prio_vset_point_selection drop column group_num;

-- Update the table to set the group number based on segment_number
UPDATE segmented_ways
SET group_num = segment_number / 10;


---- new table: eval_groups with joined geometry
drop table if exists temp_table ;

drop table if exists eval_groups ;

-- Create the new table 'groups'
CREATE TABLE eval_groups AS
WITH GroupedSegments AS (
    SELECT
        id,
        group_num,
        ST_Union(geom) AS geometry
    FROM segmented_ways
    GROUP BY id, group_num
)
SELECT
    id,
    group_num,
    geometry
FROM GroupedSegments;

CREATE INDEX eval_groups_idx ON eval_groups (id, group_num);


-- add maß_seite
ALTER TABLE eval_groups ADD COLUMN if not exists MAß_SEITE VARCHAR;

with ways as (select * from berlin_priorisierungskonzept)
update eval_groups
set MAß_SEITE = (select MAß_SEITE from ways where ways.id = eval_groups.id);



-- add group num to images

ALTER TABLE berlin_prio_vset_point_selection ADD column if not exists group_num INT;

create index if not exists segment_idx on berlin_prio_vset_point_selection(segment_id);
create index if not exists segment_ways_ids_idx on segmented_ways(segment_id);

with ways as (select * from segmented_ways)
update berlin_prio_vset_point_selection img
set group_num = (select group_num from ways where ways.segment_id = img.segment_id);


-- first group by segment_id, then by group_num
with VoteCounts AS( -- count votes per segment (by segment_id and road_type)
select
	img.way_id as id,
    img.segment_id,
    img.road_type,
    COUNT(*) as img_counts
from berlin_prio_vset_point_selection img
    GROUP BY
        img.way_id, img.segment_id, img.road_type
--  order by way_id asc
), SegmentSurfaceVotes AS ( -- votes per group by majority vote of segments per road_type
    SELECT
        img.way_id as id,
        img.group_num,
        img.segment_id,
        img.type_pred,
        img.road_type,
        to_timestamp(MIN(img.captured_at) / 1000) as min_captured_at,
        to_timestamp(MAX(img.captured_at) / 1000) as max_captured_at,
        COUNT(*) AS vote_count,
        AVG(img.type_class_prob) AS avg_class_prob
    FROM
        berlin_prio_vset_point_selection img
    WHERE img.type_pred IS NOT NULL -- and img.type_class_prob > 0.8
    GROUP BY
        img.way_id, img.group_num, img.segment_id, img.road_type, img.type_pred
), SegmentSurfaceVotes2 as ( -- join total number of images per segment (and road type) to compute confidence score
	select SSV.*,
	VC.img_counts,
	CAST(SSV.vote_count as float) / cast(VC.img_counts as float) as rt_share
	from SegmentSurfaceVotes SSV
    join VoteCounts VC
    on VC.segment_id = SSV.segment_id and VC.road_type = SSV.road_type
), RankedVotes as ( 
SELECT
		SV.*,
        RANK() OVER (PARTITION BY SV.id, SV.group_num, SV.segment_id, SV.road_type ORDER BY SV.vote_count DESC) as rank
        FROM
        SegmentSurfaceVotes2 SV
),
TopRankedVotes AS (        
    SELECT
        RV.*
    from 
    	RankedVotes RV
    WHERE  RV.rank = 1
), GroupSurfaceVotes AS (
--)
    SELECT
        TRV.id,
        TRV.group_num,
        TRV.type_pred,
        TRV.road_type,
        AVG(TRV.img_counts) as avg_img_counts,
        SUM(TRV.img_counts) as sum_img_counts,
        AVG(TRV.rt_share) as avg_rt_share,
        MIN(TRV.min_captured_at) as min_captured_at,
        MAX(TRV.max_captured_at) as max_captured_at,
        COUNT(*) AS segment_vote_count
        --AVG(TRV.type_class_prob) AS avg_class_prob -- todo: weighted avg?
    FROM
        TopRankedVotes TRV
    WHERE TRV.type_pred IS NOT NULL -- and TRV.type_class_prob > 0.8
    GROUP BY
        TRV.id, TRV.group_num, TRV.road_type, TRV.type_pred
), GroupRankedVotes as ( 
--)
    SELECT
		GV.*,
        RANK() OVER (PARTITION BY GV.id, GV.group_num, GV.road_type ORDER BY GV.segment_vote_count DESC) as rank
    FROM
        GroupSurfaceVotes GV
)
    SELECT
        GRV.*,
		ways.maß_seite,
        ways.geometry
    INTO TABLE temp_table
    FROM
        eval_groups ways
    JOIN
        GroupRankedVotes GRV
    ON
        ways.id = GRV.id and ways.group_num = GRV.group_num;
   --WHERE
   --      GRV.rank = 1; 

DROP TABLE  eval_groups;
ALTER TABLE temp_table RENAME TO eval_groups;

---- join img and groups and aggregate with majority vote
ALTER TABLE eval_groups 
ADD COLUMN if not exists avg_quality_pred FLOAT,
add column if not exists way_length float,
add column if not exists conf_score float,
add column if not exists n_segments int;


with SegmentCounts AS(
select
	img.way_id,
    img.group_num,
    img.road_type,
    COUNT(DISTINCT img.segment_id) as n_segments
from berlin_prio_vset_point_selection img
    GROUP BY
        img.way_id, img.group_num, img.road_type
)
UPDATE eval_groups ways
SET n_segments = SC.n_segments,
	way_length = ST_Length(ways.geometry),
	conf_score =  ways.avg_rt_share * (cast(segment_vote_count as float) /ceil(ST_Length(ways.geometry)/20) )
from SegmentCounts SC
WHERE ways.id = SC.way_id and ways.group_num = SC.group_num and ways.road_type = SC.road_type;


delete from eval_groups ways
where (ways.way_length / ways.n_segments  > 30) or (cast(ways.segment_vote_count as float) / cast( ways.n_segments as float) < 0.5) or sum_img_counts < 3;
-- remove predictions where there is no prediction for respective road type every 30 meters or less than 50% of segments are of predicted type or if there is less than 3 images


-- drop lower confidence score
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id, group_num, road_type ORDER BY conf_score DESC) AS row_num
    FROM
        eval_groups
),
RowsToKeep AS (
    SELECT *
    FROM RankedRows
    WHERE row_num = 1
)

DELETE FROM eval_groups
WHERE (id, group_num, conf_score) NOT IN (
    SELECT id, group_num, conf_score
    FROM RowsToKeep
);



--WHERE ways.way_length / ways.n_segments  > 30 or (ways.vote_count < (ways.n_segments * 2)); -- remove predictions where there are not 2 votes every 30 meters 

WITH QualityAvg AS (
    SELECT way_id, group_num, AVG(img.quality_pred) AS avg_quality_pred, road_type
    FROM berlin_prio_vset_point_selection img GROUP BY way_id, group_num, road_type)
UPDATE eval_groups ways
SET avg_quality_pred = QA.avg_quality_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id and ways.road_type = QA.road_type and ways.group_num=QA.group_num;



select * from eval_groups where ID = 2407;

select * from berlin_prio_vset_way_selection bpvws where ID = 2773;


select * from eval_groups where ID = 3752;
select * from berlin_prio_vset_way_selection bpvws where ID = 3752;


select * from berlin_prio_vset_way_selection bpvws where ID = 7428;

select road_type, type_pred, count(*) from berlin_prio_vset_point_selection bpvws where way_id = 7428 group by road_type, type_pred ;
