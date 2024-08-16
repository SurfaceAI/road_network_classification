-- drop table if it exists
drop table if exists {table_name_way_selection};

CREATE TABLE {table_name_way_selection} AS
SELECT * FROM {edge_table_name} WHERE ST_Within(
        geom,
        st_transform(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326), 25833)
    ); 

-- add maß_seite
ALTER TABLE {table_name_way_selection} ADD COLUMN if not exists highway VARCHAR;

with ways as (select * from berlin_priorisierungskonzept)
update {table_name_way_selection}
set highway = (select MAß_SEITE from ways where ways.id = {table_name_way_selection}.id);

alter table {table_name_way_selection} add column if not exists road_type varchar;

UPDATE {table_name_way_selection}
SET road_type = CASE
    WHEN highway = 'Beidseitig' THEN 'cycleway'
    WHEN highway IN ('Gesamte Straße', 'Gesamte Straß') THEN 'road'
    ELSE NULL
END;
