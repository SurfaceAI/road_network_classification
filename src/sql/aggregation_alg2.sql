ALTER TABLE {table_name_point_selection} ADD column if not exists group_num INT;

create index if not exists segment_idx on {table_name_point_selection} (segment_id);
create index if not exists {name}_segmented_ways_ids_idx on {name}_segmented_ways(segment_id);

-- join based on segment - not partition! (we want to seperate road types later)
with ways as (select * from {name}_segmented_ways)
update {table_name_point_selection}  img
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
        {table_name_point_selection}  img
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
        SUM(TRV.vote_count) as vote_count, -- how many votes this this surface type?
        AVG(TRV.img_counts) as avg_img_counts, -- how many imgs per subsegment on average for this road type?
        SUM(TRV.img_counts) as sum_img_counts, -- sum of all images for this road type
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
        {additional_id_column}
        ways.part_id,
        GRV.*,
		ways.road_type,
        ways.geometry
    INTO TABLE {name}_group_predictions
    FROM
        {name}_eval_groups ways
    JOIN
        GroupRankedVotes GRV
    ON
        ways.id = GRV.id and ways.group_num = GRV.group_num and 
        (ways.road_type=GRV.road_type_pred or ways.road_type is null);
        -- now we filter by partition (i.e., road type): only keep those where target road_type matches the prediction (or where there is no target)
   --WHERE
   --      GRV.rank = 1; 

-- TODO: refine matching
-- 
