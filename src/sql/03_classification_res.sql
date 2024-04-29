ALTER TABLE {table_name_point_selection}
ADD COLUMN if not exists surface_pred VARCHAR,
ADD COLUMN if not exists class_prob FLOAT;

-- Create a temporary table to hold CSV data
CREATE TEMP TABLE temp_surface_data (
    img_id VARCHAR, 
    class_prob FLOAT,
    surface_pred VARCHAR
);

-- Import data from CSV file
COPY temp_surface_data(img_id, class_prob, surface_pred)
FROM '{csv_path}'
WITH (FORMAT csv, HEADER true);


-- TODO: if there are multiple values per image (panorama image) this needs to be adjusted
-- Update `points` table by setting `surface_pred` from `temp_surface_data`
UPDATE {table_name_point_selection}
SET surface_pred = temp.surface_pred,
class_prob = temp.class_prob
FROM temp_surface_data temp
WHERE {table_name_point_selection}.img_id = temp.img_id;


