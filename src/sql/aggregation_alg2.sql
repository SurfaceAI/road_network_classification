ALTER TABLE {name}_img_metadata ADD column if not exists group_num INT;

create index if not exists segment_idx on {name}_img_metadata (segment_id);
create index if not exists {name}_segmented_ways_ids_idx on {name}_segmented_ways(segment_id);

-- join based on segment - not partition! (we want to seperate road types later)
with ways as (select * from {name}_segmented_ways)
update {name}_img_metadata  img
set group_num = (select group_num from ways where ways.segment_id = img.segment_id);

-- is the image close to a segment with partitions OR close to more than one road?
alter table {name}_img_metadata add column if not exists requ_road_type bool default true;

CREATE TEMP TABLE Partitions as (
    SELECT segment_id, MAX(part_id) AS part_nums
    FROM {name}_partitions
    GROUP BY segment_id
);
create index if not exists {name}_partitions_segment_id_idx on Partitions (segment_id);
create index if not exists {name}_img_metadata_id_idx on {name}_img_metadata (segment_id);

UPDATE {name}_img_metadata 
SET requ_road_type = CASE
    WHEN (p.part_nums > 1) or ({name}_img_metadata.num_closeby_ways > 1) THEN TRUE
    ELSE FALSE
END
FROM partitions p
WHERE {name}_img_metadata.segment_id = p.segment_id;

drop table Partitions;

-- only keep relevant imgs
drop table if exists {name}_img_selection;
create table {name}_img_selection AS(
	select img.*,
	p.part_id
	from {name}_img_metadata img
	join {name}_partitions p
	on img.segment_id = p.segment_id
	where (img.requ_road_type is false or 
		   img.road_type_pred=p.road_type) and img.road_type_pred != 'other'
);

-- TODO: when image is assigned to two partitions only keep the one thats closer
WITH RankedImages AS (
    SELECT
        img.*,
        ROW_NUMBER() OVER (
            PARTITION BY img.img_id
            ORDER BY ST_Distance(img.geom, p.geom) ASC
        ) AS row_num
    FROM
        {name}_img_selection AS img
    JOIN
        {name}_partitions AS p
    ON
        img.segment_id = p.segment_id and img.part_id = p.part_id
)
DELETE FROM {name}_img_selection img
where (img.img_id || '_' || img.part_id) in
(
    SELECT (img_id || '_' || part_id)
    FROM RankedImages
    WHERE row_num > 1
);

-- first group by segment_id, then by group_num
with VoteCounts AS( -- count votes per segment (by segment_id and road_type)
select
	img.way_id as id,
    img.segment_id,
    img.part_id,
    COUNT(*) as img_counts
from {name}_img_selection img
    GROUP BY
        img.way_id, img.segment_id, img.part_id
--  order by way_id asc
), SegmentSurfaceVotes AS ( -- votes per group by majority vote of segments per road_type
    SELECT
        img.way_id as id,
        img.group_num,
        img.segment_id,
        img.part_id,
        img.type_pred,
        to_timestamp(MIN(img.captured_at) / 1000) as min_captured_at,
        to_timestamp(MAX(img.captured_at) / 1000) as max_captured_at,
        COUNT(*) AS vote_count,
        AVG(img.type_class_prob) AS avg_class_prob
    FROM
        {name}_img_selection img
    WHERE img.type_pred IS NOT NULL -- and img.type_class_prob > 0.8
    GROUP BY
        img.way_id, img.group_num, img.segment_id, img.part_id, img.type_pred
), SegmentSurfaceVotes2 as ( -- join total number of images per segment (and road type) to compute confidence score
	select SSV.*,
	VC.img_counts,
	CAST(SSV.vote_count as float) / cast(VC.img_counts as float) as rt_share
	from SegmentSurfaceVotes SSV
    join VoteCounts VC
    on VC.segment_id = SSV.segment_id and VC.part_id = SSV.part_id
), RankedVotes as ( 
SELECT
		SV.*,
        RANK() OVER (PARTITION BY SV.id, SV.group_num, SV.segment_id, 
                                  SV.part_id ORDER BY SV.vote_count DESC) as rank
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
        TRV.part_id,
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
        TRV.id, TRV.group_num, TRV.part_id, TRV.type_pred
), GroupRankedVotes as ( 
--)
    SELECT
		GV.*,
        RANK() OVER (PARTITION BY GV.id, GV.group_num, GV.part_id ORDER BY GV.segment_vote_count DESC) as rank
    FROM
        GroupSurfaceVotes GV
)
    SELECT
        GRV.*,
		ways.road_type,
        ways.geometry
    INTO TABLE {name}_group_predictions
    FROM
        {name}_eval_groups ways
    JOIN
        GroupRankedVotes GRV
    ON
        ways.id = GRV.id and ways.group_num = GRV.group_num and  ways.part_id = GRV.part_id;
        -- now we filter by partition (i.e., road type): only keep those where target road_type matches the prediction (or where there is no target)

--  (this assumes we have a full graph of all roads, inkl. sidewalks etc)
-- is there any other road close by (xx meters)?
-- if yes: only consider target type
-- if no: consider all except the excluded ones
