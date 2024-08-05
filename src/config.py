import os

token_path = "mapillary_token.txt"
mapillary_tile_url = "https://tiles.mapillary.com/maps/vtp/{}/2/{}/{}/{}"
mapillary_graph_url = "https://graph.mapillary.com/{}"
tile_coverage = "mly1_public"
tile_layer = "image"  # "overview"
zoom = 14
parallel_batch_size = 100



#s1 = {
    #name = "s1"
    #pred_path = "test_sample-aggregation_sample-20240305_173146.csv"
    # minLon=13.4097172387
    # minLat=52.49105842
    # maxLon=13.4207674991
    # maxLat=52.4954385756
    # name = "s2"
    # pred_path = "test_sample-s2-20240415_101331.csv"
#}

weseraue = {
    "data_root": "data",
    "name": "weser_aue",
    "img_size": "thumb_2048_url",
    "pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/weseraue/prediction/effnet_surface_quality_prediction-weseraue_imgs_2048-20240617_180008.csv",
    "run": "run12",
    "minLon":8.92273,
    "minLat":52.5566,
    "maxLon":9.23894,
    "maxLat":52.7519,
    
}

berlin_prio_vset = {
    "data_root": "data",
    "name": "berlin_prio_vset",
    "img_size": "thumb_1024_url",
    "pred_path": "",
    "run": "run4",
    "minLon": 13.35108089,
    "minLat": 52.454059600,
    "maxLon": 13.43233203,
    "maxLat": 52.5186462,
    "database": "databaseBerlinPrio",
    "custom_edge_geom_table": "berlin_priorisierungskonzept",
    #"pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_surface_quality_prediction-berlin-20240704_164243.csv",
    "pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction/effnet_surface_quality_prediction-berlin_vset_all-20240716_151503.csv",
    "road_scenery_pred_path": "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/berlin/prediction//effnet_scenery_prediction-berlin_vset_all-20240718_120939.csv",
    "orig_way_id_name": "id",
    "n_per_segment": 10
}
