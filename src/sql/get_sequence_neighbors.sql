WITH context AS (
    SELECT 
        *,
        LEAD(img_id, 1) OVER (ORDER BY captured_at) AS lead1,
        LEAD(img_id, 2) OVER (ORDER BY captured_at) AS lead2,
        LEAD(img_id, 3) OVER (ORDER BY captured_at) AS lead3,
        LEAD(img_id, 4) OVER (ORDER BY captured_at) AS lead4,
        LEAD(img_id, 5) OVER (ORDER BY captured_at) AS lead5,
        LEAD(img_id, 6) OVER (ORDER BY captured_at) AS lead6,
        LEAD(img_id, 7) OVER (ORDER BY captured_at) AS lead7,
        LEAD(img_id, 8) OVER (ORDER BY captured_at) AS lead8
    FROM {name}_img_metadata
    WHERE sequence_id = '{sequence_id}'
    ORDER BY captured_at
)
SELECT ST_X(geom) as x, ST_Y(geom) as y
FROM context
WHERE img_id = '{img_id}'
   OR lead1 = '{img_id}'
   OR lead2 = '{img_id}'
   OR lead3 = '{img_id}'
   OR lead4 = '{img_id}'
   OR lead5 = '{img_id}'
   OR lead6 = '{img_id}'
   OR lead7 = '{img_id}'
   OR lead8 = '{img_id}';
  
  