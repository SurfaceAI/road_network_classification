-- drop table if it exists
drop table if exists {name}_way_selection;

CREATE TABLE {name}_way_selection AS
SELECT * FROM berlin_priorisierungskonzept WHERE ST_Within(
        geom,
        st_transform(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326), {crs})
    ); 

-- add maß_seite
ALTER TABLE {name}_way_selection ADD COLUMN if not exists highway VARCHAR;

with ways as (select * from berlin_priorisierungskonzept)
update {name}_way_selection
set highway = (select MAß_SEITE from ways where ways.id = {name}_way_selection.id);

alter table {name}_way_selection add column if not exists road_type varchar;

UPDATE {name}_way_selection
SET road_type = CASE
    WHEN highway = 'Beidseitig' THEN 'cycleway'
    WHEN highway IN ('Gesamte Straße', 'Gesamte Straß') THEN 'road'
    ELSE NULL
END;
