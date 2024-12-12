
---- join img and groups and aggregate with majority vote
ALTER TABLE {name}_group_predictions
add column if not exists quali_pred float,
add column if not exists way_length float,
add column if not exists conf_score float,
add column if not exists n_segments int;


with SegmentCounts AS(
select
	img.way_id,
    img.group_num,
    img.part_id,
    COUNT(DISTINCT img.segment_id) as n_segments
from {name}_img_selection img
    GROUP BY
        img.way_id, img.group_num, img.part_id
)
UPDATE {name}_group_predictions ways
SET n_segments = SC.n_segments,
	way_length = ST_Length(ways.geometry),
	conf_score =  ways.avg_rt_share * (cast(segment_vote_count as float) /ceil(ST_Length(ways.geometry)/20) )
from SegmentCounts SC
WHERE ways.id = SC.way_id and ways.group_num = SC.group_num and ways.part_id = SC.part_id;


delete from {name}_group_predictions ways
where (ways.way_length / ways.n_segments  > 30) 
    or (cast(ways.segment_vote_count as float) / cast( ways.n_segments as float) < 0.5) 
    or n_imgs < 3;
-- remove predictions where there is no prediction for respective road type every 30 meters 
-- or less than 50% of segments are of predicted type 
-- or if there is less than 3 images


-- drop lower confidence score
WITH RankedRows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY id, group_num, part_id ORDER BY conf_score DESC) AS row_num
    FROM
        {name}_group_predictions
),
RowsToKeep AS (
    SELECT *
    FROM RankedRows
    WHERE row_num = 1
)
DELETE FROM {name}_group_predictions
WHERE (id, group_num, part_id, conf_score) NOT IN (
    SELECT id, group_num, part_id, conf_score
    FROM RowsToKeep
);


-- quality info
WITH QualityAvg AS (
    SELECT way_id, group_num, part_id, AVG(img.quality_pred) AS quali_pred, type_pred
    FROM {name}_img_selection img GROUP BY way_id, group_num, part_id, type_pred)
UPDATE {name}_group_predictions ways
SET quali_pred = QA.quali_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id 
    and ways.part_id = QA.part_id 
    and ways.type_pred = QA.type_pred 
    and ways.group_num=QA.group_num;


-- add groups with no value as placeholders
with MissingGroups as (
select *
from {name}_eval_groups gr
where (gr.id || '_' || gr.part_id  || '_' || gr.group_num)  not in 
(select (gp.id || '_' || gp.part_id  || '_' || gp.group_num)  from {name}_group_predictions gp)
)
insert into {name}_group_predictions ({grouping_ids}, road_type, geometry) -- elem_nr, id, group_num, part_id,
select {grouping_ids}, road_type, geometry from MissingGroups;
--select MG.id, MG.group_num, MG.part_id, MG.road_type, MG.geometry from MissingGroups as MG;

-- only keep one prediction for each way (if multiple road types are specified as target, i.e., via partition id
-- then keep one prediction per partition)
-- keep the one with most segment vote counts // then with most average image counts
-- this is only needed if no target type is given (typically not an issue for OSM)
CREATE TABLE RowsToKeep as
 WITH RankedRows AS (
    SELECT *,
            st_transform(geometry, 4326) AS geom_4326,
           row_number() OVER (
               PARTITION BY {grouping_ids}
               ORDER BY segment_vote_count DESC, avg_img_counts DESC
           ) AS row_num
    FROM {name}_group_predictions
)  
    SELECT {grouping_ids}, type_pred, 
    segment_vote_count, avg_rt_share, conf_score, 
    quali_pred, 
    road_type, 
    n_segments, avg_img_counts, n_imgs, 
    min_date, max_date, geom_4326 as geometry
    FROM RankedRows
    WHERE row_num = 1
 ;

DROP TABLE  {name}_group_predictions;
ALTER TABLE RowsToKeep RENAME TO {name}_group_predictions;




