-- add maß_seite
ALTER TABLE ways ADD column IF NOT EXISTS road_type VARCHAR;

UPDATE ways
SET road_type = CASE
    WHEN MAß_SEITE = 'Beidseitig' THEN 'cycleway'
    WHEN MAß_SEITE IN ('Gesamte Straße', 'Gesamte Straß') THEN 'road'
    ELSE NULL
END;

-- rename geom column
AlTER TABLE ways RENAME COLUMN wkb_geometry TO geom;
