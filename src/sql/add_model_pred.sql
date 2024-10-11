ALTER TABLE  {name}_img_metadata
    ADD COLUMN IF NOT EXISTS road_type_pred VARCHAR,
    ADD COLUMN IF NOT EXISTS road_type_prob double precision,
    ADD COLUMN IF NOT EXISTS type_pred VARCHAR,
    ADD COLUMN IF NOT EXISTS type_class_prob double precision,
    ADD COLUMN IF NOT EXISTS quality_pred double precision;

UPDATE {name}_img_metadata
    SET road_type_pred = %s,
    road_type_prob = %s,
    type_pred = %s,
    type_class_prob = %s,
    quality_pred = %s
    FROM {name}_img_metadata img
    WHERE %s = img.img_id;
