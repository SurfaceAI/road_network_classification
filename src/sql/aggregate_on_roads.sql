-- add atribute to sample_way_geometry with majority vote
ALTER TABLE {table_name_way_selection} 
--ADD COLUMN if not exists road_type VARCHAR,
--ADD COLUMN if not exists type_pred VARCHAR,
--ADD COLUMN if not exists avg_class_prob FLOAT,
ADD COLUMN if not exists avg_quality_pred FLOAT,
add column if not exists way_length float,
add column if not exists n_segments int;
--add column if not exists min_captured_at timestamp,
--add column if not exists max_captured_at timestamp;

DROP TABLE IF EXISTS temp_table;

ALTER TABLE {table_name_way_selection} 
DROP COLUMN if exists road_type,
DROP COLUMN if exists type_pred,
DROP COLUMN if exists avg_class_prob,
DROP COLUMN if exists vote_count,
DROP COLUMN if exists min_captured_at,
DROP COLUMN if exists max_captured_at;

WITH SurfaceVotes AS (
    SELECT
        img.way_id,
        img.type_pred,
        img.road_type,
        to_timestamp(MIN(img.captured_at) / 1000) as min_captured_at,
        to_timestamp(MAX(img.captured_at) / 1000) as max_captured_at,
        COUNT(*) AS vote_count,
        AVG(img.type_class_prob) AS avg_class_prob
    FROM
        {table_name_point_selection} img
    WHERE img.type_pred IS NOT NULL -- and img.type_class_prob > 0.8
    GROUP BY
        img.way_id, img.type_pred, img.road_type
), RankedVotes AS (
    SELECT
        SV.way_id,
        SV.road_type,
        SV.type_pred,
        SV.avg_class_prob,
        SV.vote_count,
        SV.min_captured_at,
        SV.max_captured_at,
        RANK() OVER (PARTITION BY SV.way_id, SV.road_type ORDER BY SV.vote_count DESC) as rank
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
        {table_name_way_selection} ways
    JOIN
        RankedVotes RV
    ON
        ways.id = RV.way_id
    WHERE
        RV.rank = 1; 

DROP TABLE  {table_name_way_selection};
ALTER TABLE temp_table RENAME TO {table_name_way_selection};

with SegmentCounts AS(
select
	img.way_id,
    img.road_type,
    COUNT(DISTINCT img.segment_id) as n_segments
from {table_name_point_selection} img
    GROUP BY
        img.way_id, img.road_type
)
UPDATE {table_name_way_selection} ways
SET n_segments = SC.n_segments,
	way_length = ST_Length(ways.geom)
from SegmentCounts SC
WHERE ways.id = SC.way_id and ways.road_type = SC.road_type;

delete from {table_name_way_selection} ways
WHERE ways.way_length / ways.n_segments  > 30 or (ways.vote_count < (ways.n_segments * 2)); -- remove predictions where there are not 2 votes every 30 meters 

-- TODO: instead cut segment?


WITH QualityAvg AS (
    SELECT way_id, AVG(img.quality_pred) AS avg_quality_pred, road_type
    FROM {table_name_point_selection} img GROUP BY way_id, road_type)
UPDATE {table_name_way_selection} ways
SET avg_quality_pred = QA.avg_quality_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id and ways.road_type = QA.road_type;

