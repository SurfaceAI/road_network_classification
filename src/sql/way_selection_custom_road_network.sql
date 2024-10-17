-- drop table if it exists
drop table if exists {name}_way_selection;

CREATE TABLE {name}_way_selection AS
SELECT * FROM ways WHERE ST_Within(
        geom,
        st_transform(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326), {crs})
    ); 

alter table {name}_way_selection add column if not exists road_type VARCHAR;
