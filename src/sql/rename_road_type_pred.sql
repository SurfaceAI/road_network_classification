    
                             UPDATE {name}_img_metadata
                                set road_type_pred =
                                CASE
                                    WHEN road_type_pred LIKE '1_1_road%' THEN 'road'
                                    WHEN road_type_pred LIKE '1_4_path%' THEN 'path'
                                    WHEN road_type_pred LIKE '1_3_pedestrian%' THEN 'footway'
                                    WHEN road_type_pred LIKE '1_2_bicycle__1_2_lane' THEN 'bike_lane'
                                    WHEN road_type_pred LIKE '1_2_bicycle__1_2_cycleway' THEN 'cycleway'
                                    ELSE 'other'
                                END;