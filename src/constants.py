import os

# sql file names
from pathlib import Path

root_path = Path(os.path.abspath(__file__)).parent.parent


SQL_FOLDER = root_path / "src" / "sql"
SQL_CREATE_IMG_METADATA_TABLE = SQL_FOLDER / "create_img_metadata_table.sql"
SQL_ADD_GEOM_COLUMN = SQL_FOLDER / "add_geom_column.sql"
SQL_WAY_SELECTION = SQL_FOLDER / "way_selection.sql"
SQL_WAY_SELECTION_CUSTOM = SQL_FOLDER / "way_selection_custom_road_network.sql"
SQL_SEGMENT_WAYS = SQL_FOLDER / "segment_ways.sql"
# SQL_IMG_SELECTION = SQL_FOLDER / "img_selection.sql"
SQL_MATCH_IMG_ROADS = SQL_FOLDER / "match_imgs_to_segments.sql"
SQL_PREPARE_PARTITIONS = SQL_FOLDER / "prepare_partitions.sql"
SQL_PREP_MODEL_RESULT = SQL_FOLDER / "prepare_model_result_insert.sql"
SQL_RENAME_ROAD_TYPE_PRED = SQL_FOLDER / "rename_road_type_pred.sql"
SQL_AGGREGATE_ON_ROADS = SQL_FOLDER / "aggregation_alg{}.sql"
SQL_SEPARATE_ROAD_TYPES = SQL_FOLDER / "separate_road_types.sql"
SQL_SEPARATE_NULL_ROAD_TYPES = SQL_FOLDER / "separate_null_road_types.sql"

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
