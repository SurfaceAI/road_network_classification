import os

# mapillary token file
token_path = "mapillary_token.txt"

# parallelized image download
parallel_batch_size = 100

global_config = {
    "data_root" : "data",
    "database": "database",
    "pbf_path" : "data/germany-latest.osm.pbf",
    "img_size": "thumb_2048_url",
    "n_per_segment" : None,
    "dist_from_road": 10,
    "segment_length": 20,
    "min_road_length" : 10,
    "segments_per_group": 100,
    "img_selection_csv_path": "img_selection.csv",
}

weseraue = {
    **global_config,
    "name": "weser_aue",
    "pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/weseraue/prediction/effnet_surface_quality_prediction-weseraue_imgs_2048-20240617_180008.csv",
    "run": "run12",
    "minLon":8.92273,
    "minLat":52.5566,
    "maxLon":9.23894,
    "maxLat":52.7519,
    
}

berlin_prio_vset = {
    **global_config,
    "name": "berlin_prio_vset",
    "run": "run4",
    "minLon": 13.35108089,
    "minLat": 52.454059600,
    "maxLon": 13.43233203,
    "maxLat": 52.5186462,
    "database": "databaseBerlinPrio",
    "pbf_path": None,
    "custom_sql_way_selection": "src/sql/way_selection_berlin.sql",
    "custom_road_type_separation": "src/sql/assign_road_types_berlin.sql",
    "crs": 25833,
    "custom_attrs":{"edge_table_name": "berlin_priorisierungskonzept"},
    "pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_surface_quality_prediction-berlin_vset_all-20240716_151503.csv",
    "road_scenery_pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_scenery_prediction-berlin_vset_all-20240718_120939.csv",
}


berlin_prio = {
    **global_config,
    "data_root": "data",
    "name": "berlin_prio",
    "run": "run1",
    "minLon": 13.090211,
    "minLat": 52.377425,
    "maxLon": 13.7416329,
    "maxLat": 52.660392,
    "database": "databaseBerlinPrio",
    "pbf_path": None,
    "custom_sql_way_selection": "src/sql/way_selection_berlin.sql",
    "crs": 25833,
    "custom_attrs":{"edge_table_name": "berlin_priorisierungskonzept"},
    "custom_road_type_separation": "src/sql/assign_road_types_berlin.sql",
    "pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_surface_quality_prediction-berlin_all-20240810_170444.csv",
    "road_scenery_pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_scenery_prediction-berlin_all-20240811_053246.csv",
    "min_road_length" : 0,
    "segments_per_group": 1000,

}

dresden = {
    **global_config,
    "name": "dresden",
    "run": "run1",
    "minLon": 13.654175,
    "minLat": 51.00662,
    "maxLon": 13.828583,
    "maxLat": 51.094676,
    "pred_path": "",
    "road_scenery_pred_path": "",
}

dresden_small = {
    **global_config,
    "name": "dresden_small",
    "run": "run1",
    "minLon": 13.735721,
    "minLat": 51.049871,
    "maxLon": 13.752246,
    "maxLat": 51.05792,
    "pred_path": "",
    "road_scenery_pred_path": "",
}


