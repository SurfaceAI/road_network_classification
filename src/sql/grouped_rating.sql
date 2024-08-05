-- create a grouping variable in segments
ALTER TABLE segmented_ways ADD COLUMN if not exists group_num INT;

--alter table berlin_prio_vset_point_selection drop column group_num;

-- Update the table to set the group number based on segment_number
UPDATE segmented_ways
SET group_num = segment_number / 100;


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


---- join img and groups and aggregate with majority vote
ALTER TABLE eval_groups 
ADD COLUMN if not exists avg_quality_pred FLOAT,
add column if not exists way_length float,
add column if not exists n_segments int;

DROP TABLE IF EXISTS temp_table;

ALTER TABLE eval_groups
DROP COLUMN if exists road_type,
DROP COLUMN if exists type_pred,
DROP COLUMN if exists avg_class_prob,
DROP COLUMN if exists vote_count,
DROP COLUMN if exists min_captured_at,
DROP COLUMN if exists max_captured_at;

WITH SurfaceVotes AS (
    SELECT
        img.way_id,
        img.group_num,
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
        img.way_id, img.group_num, img.road_type, img.type_pred
), RankedVotes AS (
    SELECT
        SV.way_id,
        SV.group_num,
        SV.road_type,
        SV.type_pred,
        SV.avg_class_prob,
        SV.vote_count,
        SV.min_captured_at,
        SV.max_captured_at,
        RANK() OVER (PARTITION BY SV.way_id, SV.group_num, SV.road_type ORDER BY SV.vote_count DESC) as rank
    FROM
        SurfaceVotes SV
)
    SELECT
        ways.*,
        RV.road_type,
        RV.type_pred,
        RV.avg_class_prob,
        RV.vote_count,
        RV.min_captured_at,
        RV.max_captured_at
    INTO TABLE temp_table
    FROM
        eval_groups ways
    JOIN
        RankedVotes RV
    ON
        ways.id = RV.way_id and ways.group_num = RV.group_num
    WHERE
        RV.rank = 1; 

DROP TABLE  eval_groups;
ALTER TABLE temp_table RENAME TO eval_groups;


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
	way_length = ST_Length(ways.geometry)
from SegmentCounts SC
WHERE ways.id = SC.way_id and ways.road_type = SC.road_type;


delete from eval_groups ways
WHERE ways.way_length / ways.n_segments  > 30 or (ways.vote_count < (ways.n_segments * 2)); -- remove predictions where there are not 2 votes every 30 meters 

WITH QualityAvg AS (
    SELECT way_id, group_num, AVG(img.quality_pred) AS avg_quality_pred, road_type
    FROM berlin_prio_vset_point_selection img GROUP BY way_id, group_num, road_type)
UPDATE eval_groups ways
SET avg_quality_pred = QA.avg_quality_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id and ways.road_type = QA.road_type and ways.group_num=QA.group_num;

