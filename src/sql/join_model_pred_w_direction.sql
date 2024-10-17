-- Create a temporary table to hold CSV data
CREATE TEMP TABLE temp_pred (
    img_id VARCHAR, 
    direction INT,
    type_pred VARCHAR,
    type_class_prob FLOAT,
    quality_pred FLOAT,
    quality_pred_label VARCHAR
);

-- Import data from CSV file
(COPY temp_pred(img_id,direction,type_pred,type_class_prob,quality_pred,quality_pred_label)
FROM '{surface_pred_csv_path}'
WITH (FORMAT csv, HEADER true));

DROP TABLE IF EXISTS temp_pt_selection;

alter table {name}_img_metadata 
drop column if exists type_pred, 
drop column if exists type_class_prob,
drop column if exists quality_pred,
drop column if exists quality_pred_label;

-- Update `points` table by setting `surface_pred` from `temp_pred`
create TABLE temp_pt_selection AS
SELECT A.*, B.type_pred, B.type_class_prob, B.quality_pred, B.quality_pred_label
FROM  {name}_img_metadata A
LEFT JOIN temp_pred B on A.img_id = B.img_id and A.direction = B.direction;

DROP TABLE  {name}_img_metadata;
ALTER TABLE temp_pt_selection RENAME TO  {name}_img_metadata;