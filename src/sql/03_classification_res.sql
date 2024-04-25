ALTER TABLE {table_name_point_selection}
ADD COLUMN surface_pred VARCHAR;

-- Create a temporary table to hold CSV data
CREATE TEMP TABLE temp_surface_data (
    img_id VARCHAR, 
    class_prob FLOAT,
    surface_pred VARCHAR,
    is_in_valid BOOLEAN
);

-- Import data from CSV file
COPY temp_surface_data(img_id, class_prob, surface_pred, is_in_valid)
FROM '{csv_path}'
WITH (FORMAT csv, HEADER true);

-- Update `points` table by setting `surface_pred` from `temp_surface_data`
UPDATE {table_name_point_selection}
SET surface_pred = temp.surface_pred
FROM temp_surface_data temp
WHERE {table_name_point_selection}.img_id = temp.img_id;


