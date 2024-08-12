import os 

# sql file names

SQL_FOLDER = os.path.join("src", "sql")
SQL_IMGS_TO_DB = os.path.join(SQL_FOLDER, "imgs_to_db.sql")
SQL_WAY_SELECTION = os.path.join(SQL_FOLDER, "way_selection.sql")
SQL_SEGMENT_WAYS = os.path.join(SQL_FOLDER, "segment_ways.sql")
SQL_IMG_SELECTION = os.path.join(SQL_FOLDER, "img_selection.sql")
SQL_MATCH_IMG_ROADS = os.path.join(SQL_FOLDER, "match_imgs_to_segments.sql")
SQL_MATCH_IMG_ROADS_CUST = os.path.join(SQL_FOLDER, "intersect_img_custom_ways.sql")
SQL_JOIN_MODEL_PRED = os.path.join(SQL_FOLDER, "join_model_pred.sql")
SQL_JOIN_MODEL_PRED_DIR = os.path.join(SQL_FOLDER, "join_model_pred_w_dir.sql")
SQL_JOIN_SCENERY_PRED = os.path.join(SQL_FOLDER, "join_scenery_pred.sql")
SQL_AGGREGATE_ON_ROADS = os.path.join(SQL_FOLDER, "aggregate_on_roads.sql")
SQL_ASSIGN_ROAD_TYPES =  os.path.join(SQL_FOLDER, "assign_road_types.sql")


# Mapilary settings
MAPILLARY_TILE_URL = "https://tiles.mapillary.com/maps/vtp/{}/2/{}/{}/{}"
MAPILLARY_GRAPH_URL = "https://graph.mapillary.com/{}"
TILE_COVERAGE = "mly1_public"
TILE_LAYER = "image"  # "overview"
ZOOM = 14