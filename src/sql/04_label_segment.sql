-- add atribute to sample_way_geometry with majority vote
ALTER TABLE sample_way_geometry 
ADD COLUMN if not exists surface_pred VARCHAR,
ADD COLUMN if not exists avg_class_prob FLOAT;

WITH SurfaceVotes AS (
    SELECT
        img.way_id,
        img.surface_pred,
        COUNT(*) AS vote_count,
        AVG(img.class_prob) AS avg_class_prob
    FROM
        {table_name_point_selection} img
    GROUP BY
        img.way_id, img.surface_pred
), RankedVotes AS (
    SELECT
        SV.way_id,
        SV.surface_pred,
        SV.avg_class_prob,
        RANK() OVER (PARTITION BY SV.way_id ORDER BY SV.vote_count DESC) as rank
    FROM
        SurfaceVotes SV
)
UPDATE sample_way_geometry ways
SET surface_pred = RV.surface_pred,
avg_class_prob = RV.avg_class_prob
FROM RankedVotes RV
WHERE ways.id = RV.way_id AND RV.rank = 1;

