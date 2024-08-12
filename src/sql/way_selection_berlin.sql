-- drop table if it exists
drop table if exists {table_name_way_selection};

CREATE TABLE {table_name_way_selection} AS
SELECT * FROM {edge_table_name} WHERE ST_Within(
        geom,
        st_transform(ST_MakeEnvelope({bbox0}, {bbox1}, {bbox2}, {bbox3}, 4326), 25833)
    ); 
