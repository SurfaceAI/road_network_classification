import os 

# sql file names

SQL_FOLDER = os.path.join("src", "sql")
SQL_IMGS_TO_DB = os.path.join(SQL_FOLDER, "imgs_to_db.sql")
SQL_LINE_SEGMENTS = os.path.join(SQL_FOLDER, "line_segments.sql")
SQL_LINE_SEGMENTS_CUST = os.path.join(SQL_FOLDER, "line_segments_custom.sql")
SQL_IMG_SELECTION = os.path.join(SQL_FOLDER, "img_selection.sql")
SQL_MATCH_IMG_ROADS = os.path.join(SQL_FOLDER, "intersect_img_osm_ways.sql")
SQL_MATCH_IMG_ROADS_CUST = os.path.join(SQL_FOLDER, "intersect_img_custom_ways.sql")
SQL_JOIN_MODEL_PRED = os.path.join(SQL_FOLDER, "join_model_pred.sql")
SQL_JOIN_MODEL_PRED_DIR = os.path.join(SQL_FOLDER, "join_model_pred_w_dir.sql")
SQL_JOIN_SCENERY_PRED = os.path.join(SQL_FOLDER, "join_scenery_pred.sql")