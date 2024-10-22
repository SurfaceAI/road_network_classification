alter table {name}_way_selection add column if not exists surface_clean VARCHAR;

UPDATE {name}_way_selection
SET surface_clean = CASE 
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
                   