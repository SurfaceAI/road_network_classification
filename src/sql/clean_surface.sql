alter table sample_way_geometry drop column osm_surface_clean;

alter table sample_way_geometry add column if not exists osm_surface_clean VARCHAR;


update sample_way_geometry set osm_surface_clean = surface;


UPDATE sample_way_geometry
SET osm_surface_clean = CASE 
                        WHEN surface = 'compacted' THEN 'unpaved'
                        WHEN surface = 'gravel' THEN 'unpaved'
                        WHEN surface = 'ground' THEN 'unpaved'
                        when surface = 'ground;grass;sand' THEN 'unpaved'
                        WHEN surface = 'fine_gravel' THEN 'unpaved'
                        WHEN surface = 'dirt' THEN 'unpaved'
                        WHEN surface = 'grass' THEN 'unpaved'
                        WHEN surface = 'earth' THEN 'unpaved'
                        WHEN surface = 'sand' THEN 'unpaved'
                        when surface = 'pebblestone' then 'unpaved'
                        WHEN surface = 'unhewn_cobblestone' THEN 'sett'
                        WHEN surface = 'cobblestone' then 'sett'
                        WHEN surface = 'concrete:lanes' THEN 'concrete'
                        WHEN surface = 'concrete:plates' THEN 'concrete'
                        when surface = 'wood' then 'other'
                        when surface = 'grass_paver' then 'other'
                        ELSE surface
                    END;
                   

select osm_surface_clean, count(*) from sample_way_geometry swg group by osm_surface_clean;