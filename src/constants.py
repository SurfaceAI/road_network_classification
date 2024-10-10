import os

# sql file names
from pathlib import Path

root_path = str(Path(os.path.abspath(__file__)).parent.parent)


SQL_FOLDER = os.path.join(root_path, "src", "sql")
SQL_IMGS_TO_DB = os.path.join(SQL_FOLDER, "imgs_to_db.sql")
SQL_CREATE_IMG_METADATA_TABLE = os.path.join(SQL_FOLDER, "create_img_metadata_table.sql")
SQL_ADD_GEOM_COLUMN = os.path.join(SQL_FOLDER, "add_geom_column.sql")
SQL_WAY_SELECTION = os.path.join(SQL_FOLDER, "way_selection.sql")
SQL_SEGMENT_WAYS = os.path.join(SQL_FOLDER, "segment_ways.sql")
SQL_IMG_SELECTION = os.path.join(SQL_FOLDER, "img_selection.sql")
SQL_MATCH_IMG_ROADS = os.path.join(SQL_FOLDER, "match_imgs_to_segments.sql")
SQL_MATCH_IMG_ROADS_CUST = os.path.join(SQL_FOLDER, "intersect_img_custom_ways.sql")
SQL_JOIN_MODEL_PRED = os.path.join(SQL_FOLDER, "join_model_pred.sql")
SQL_JOIN_MODEL_PRED_DIR = os.path.join(SQL_FOLDER, "join_model_pred_w_dir.sql")
SQL_JOIN_TYPE_PRED = os.path.join(SQL_FOLDER, "join_type_pred.sql")
SQL_AGGREGATE_ON_ROADS = os.path.join(SQL_FOLDER, "aggregation_alg{}.sql")
SQL_ASSIGN_ROAD_TYPES = os.path.join(SQL_FOLDER, "assign_road_types.sql")


# Mapilary settings
MAPILLARY_TILE_URL = "https://tiles.mapillary.com/maps/vtp/{}/2/{}/{}/{}"
MAPILLARY_GRAPH_URL = "https://graph.mapillary.com/{}"
TILE_COVERAGE = "mly1_public"
TILE_LAYER = "image"  # "overview"
ZOOM = 14

# Model settings
EFFNET_LINEAR = "efficientNetV2SLinear"
CROP_LOWER_MIDDLE_THIRD = "lower_middle_third"
CROP_LOWER_MIDDLE_HALF = "lower_middle_half"
CROP_LOWER_HALF = "lower_half"
NORM_MEAN = [0.42834484577178955, 0.4461250305175781, 0.4350937306880951]
NORM_SD = [0.22991590201854706, 0.23555299639701843, 0.26348039507865906]