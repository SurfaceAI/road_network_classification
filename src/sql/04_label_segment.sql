-- add atribute to sample_way_geometry with majority vote
ALTER TABLE {table_name_way_selection} 
ADD COLUMN if not exists type_pred VARCHAR,
ADD COLUMN if not exists avg_class_prob FLOAT,
ADD COLUMN if not exists quality_pred VARCHAR,
ADD COLUMN if not exists avg_quality_pred FLOAT,
add column if not exists way_length float,
add column if not exists n_segments int;

-- aggregate over segments first, then ways
WITH SurfaceVotes AS (
    SELECT
        img.way_id,
        img.type_pred,
        COUNT(*) AS vote_count,
        AVG(img.type_class_prob) AS avg_class_prob
    FROM
        {table_name_point_selection} img
    WHERE img.type_pred IS NOT NULL -- and img.type_class_prob > 0.8
    GROUP BY
        img.way_id, img.type_pred
), RankedVotes AS (
    SELECT
        SV.way_id,
        SV.type_pred,
        SV.avg_class_prob,
        RANK() OVER (PARTITION BY SV.way_id ORDER BY SV.vote_count DESC) as rank
    FROM
        SurfaceVotes SV
)
UPDATE {table_name_way_selection} ways
SET type_pred = RV.type_pred,
avg_class_prob = RV.avg_class_prob
FROM RankedVotes RV
WHERE ways.id = RV.way_id AND RV.rank = 1;

with SegmentCounts AS(
select
	img.way_id,
    COUNT(DISTINCT img.segment_id) as n_segments
from {table_name_point_selection} img
    GROUP BY
        img.way_id
)
UPDATE {table_name_way_selection} ways
SET n_segments = SC.n_segments,
	way_length = ST_Length(ways.geom)
from SegmentCounts SC
WHERE ways.id = SC.way_id;

UPDATE {table_name_way_selection} ways
SET type_pred = NULL,
avg_class_prob = NULL
WHERE ways.way_length / ways.n_segments  > 25; -- remove predictions where there are not predictions every 30 meters


WITH QualityAvg AS (
    SELECT way_id, AVG(img.quality_pred) AS avg_quality_pred 
    FROM {table_name_point_selection} img GROUP BY way_id)
UPDATE {table_name_way_selection} ways
SET avg_quality_pred = QA.avg_quality_pred
FROM QualityAvg QA
WHERE ways.id = QA.way_id;

