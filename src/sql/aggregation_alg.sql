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
CREATE TABLE {table_name_eval_groups} AS
WITH GroupedSegments AS (
    SELECT
        id,
        part_id,
        group_num,
        road_type,
        ST_Union(geom) AS geometry
    FROM {table_name_segmented_ways}
    GROUP BY id, part_id, group_num, road_type
)
SELECT
    id,
    part_id,
    group_num,
    road_type,
    geometry
FROM GroupedSegments;

CREATE INDEX {table_name_eval_groups}_idx ON {table_name_eval_groups} (id, part_id, group_num);

-- add group num to images

ALTER TABLE {table_name_point_selection} ADD column if not exists group_num INT;

create index if not exists segment_idx on {table_name_point_selection}(segment_id);
create index if not exists {table_name_segmented_ways}_ids_idx on {table_name_segmented_ways}(segment_id);

-- join based on segment - not partition! (we want to seperate road types later)
with ways as (select * from {table_name_segmented_ways})
update {table_name_point_selection} img
set group_num = (select group_num from ways where ways.segment_id = img.segment_id);


-- first group by segment_id, then by group_num
with VoteCounts AS( -- count votes per segment (by segment_id and road_type)
select
	img.way_id as id,
    img.segment_id,
    img.road_type_pred,
    COUNT(*) as img_counts
from {table_name_point_selection} img
    GROUP BY
        img.way_id, img.segment_id, img.road_type_pred
--  order by way_id asc
), SegmentSurfaceVotes AS ( -- votes per group by majority vote of segments per road_type
    SELECT
        img.way_id as id,
        img.group_num,
        img.segment_id,
        img.type_pred,
        img.road_type_pred,
        to_timestamp(MIN(img.captured_at) / 1000) as min_captured_at,
        to_timestamp(MAX(img.captured_at) / 1000) as max_captured_at,
        COUNT(*) AS vote_count,
        AVG(img.type_class_prob) AS avg_class_prob
    FROM
        {table_name_point_selection} img
    WHERE img.type_pred IS NOT NULL -- and img.type_class_prob > 0.8
    GROUP BY
        img.way_id, img.group_num, img.segment_id, img.road_type_pred, img.type_pred
), SegmentSurfaceVotes2 as ( -- join total number of images per segment (and road type) to compute confidence score
	select SSV.*,
	VC.img_counts,
	CAST(SSV.vote_count as float) / cast(VC.img_counts as float) as rt_share
	from SegmentSurfaceVotes SSV
    join VoteCounts VC
    on VC.segment_id = SSV.segment_id and VC.road_type_pred = SSV.road_type_pred
), RankedVotes as ( 
SELECT
		SV.*,
        RANK() OVER (PARTITION BY SV.id, SV.group_num, SV.segment_id, SV.road_type_pred ORDER BY SV.vote_count DESC) as rank
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
        TRV.road_type_pred,
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
        TRV.id, TRV.group_num, TRV.road_type_pred, TRV.type_pred
), GroupRankedVotes as ( 
--)
    SELECT
		GV.*,
        RANK() OVER (PARTITION BY GV.id, GV.group_num, GV.road_type_pred ORDER BY GV.segment_vote_count DESC) as rank
    FROM
        GroupSurfaceVotes GV
)
    SELECT
        GRV.*,
        ways.part_id,
		ways.road_type,
        ways.geometry
    INTO TABLE {table_name_group_predictions}
    FROM
        {table_name_eval_groups} ways
    JOIN
        GroupRankedVotes GRV
    ON
        ways.id = GRV.id and ways.group_num = GRV.group_num and (ways.road_type=GRV.road_type_pred or ways.road_type is null);
        -- now we filter by partition: only keep those where target road_type matches the prediction (or where there is no target)
   --WHERE
   --      GRV.rank = 1; 


---- join img and groups and aggregate with majority vote
ALTER TABLE {table_name_group_predictions}
ADD COLUMN if not exists avg_quality_pred FLOAT,
add column if not exists way_length float,
add column if not exists conf_score float,
add column if not exists n_segments int;


with SegmentCounts AS(
select
	img.way_id,
    img.group_num,
    img.road_type_pred,
    COUNT(DISTINCT img.segment_id) as n_segments
from {table_name_point_selection} img
    GROUP BY
        img.way_id, img.group_num, img.road_type_pred
)
UPDATE {table_name_group_predictions} ways
SET n_segments = SC.n_segments,
	way_length = ST_Length(ways.geometry),
	conf_score =  ways.avg_rt_share * (cast(segment_vote_count as float) /ceil(ST_Length(ways.geometry)/20) )
from SegmentCounts SC
WHERE ways.id = SC.way_id and ways.group_num = SC.group_num and ways.road_type_pred = SC.road_type_pred;


delete from {table_name_group_predictions} ways
where (ways.way_length / ways.n_segments  > 30) 
    or (cast(ways.segment_vote_count as float) / cast( ways.n_segments as float) < 0.5) 
    or sum_img_counts < 3;
-- remove predictions where there is no prediction for respective road type every 30 meters 
-- or less than 50% of segments are of predicted type 
-- or if there is less than 3 images


-- drop lower confidence score
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id, group_num, road_type_pred ORDER BY conf_score DESC) AS row_num
    FROM
        {table_name_group_predictions}
),
RowsToKeep AS (
    SELECT *
    FROM RankedRows
    WHERE row_num = 1
)

DELETE FROM {table_name_group_predictions}
WHERE (id, group_num, conf_score) NOT IN (
    SELECT id, group_num, conf_score
    FROM RowsToKeep
);


-- quality info

WITH QualityAvg AS (
    SELECT way_id, group_num, AVG(img.quality_pred) AS avg_quality_pred, road_type_pred, type_pred
    FROM {table_name_point_selection} img GROUP BY way_id, group_num, road_type_pred, type_pred)
UPDATE {table_name_group_predictions} ways
SET avg_quality_pred = QA.avg_quality_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id and ways.road_type_pred = QA.road_type_pred and ways.type_pred = QA.type_pred and ways.group_num=QA.group_num;


-- add groups with no value as placeholders
with MissingGroups as (
select *
from {table_name_eval_groups} gr
where (gr.id || '_' || gr.part_id  || '_' || gr.group_num)  not in 
(select (gp.id || '_' || gp.part_id  || '_' || gp.group_num)  from {table_name_group_predictions} gp)
)
insert into {table_name_group_predictions} (id, group_num, part_id, road_type, geometry) 
select MG.id, MG.group_num, MG.part_id, MG.road_type, MG.geometry from MissingGroups as MG;

-- only keep one prediction for each way (if multiple road types are specified as target, i.e., via partition id
-- then keep one prediciton per partition)
-- keep the one with most segment vote counts // then with most average image counts
-- this is only needed of no target type is given (typically not an issue for OSM)
CREATE TABLE RowsToKeep as
 WITH RankedRows AS (
    SELECT *,
           row_number() OVER (
               PARTITION BY id, group_num, part_id
               ORDER BY segment_vote_count DESC, avg_img_counts DESC
           ) AS row_num
    FROM {table_name_group_predictions}
)  
    SELECT id, group_num, part_id, type_pred, 
    segment_vote_count, avg_rt_share, conf_score, 
    avg_quality_pred, 
    road_type_pred, road_type, 
    n_segments, avg_img_counts, sum_img_counts, 
    min_captured_at, max_captured_at, geometry
    FROM RankedRows
    WHERE row_num = 1
 ;

DROP TABLE  {table_name_group_predictions};
ALTER TABLE RowsToKeep RENAME TO {table_name_group_predictions};




