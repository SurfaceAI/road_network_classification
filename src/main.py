import argparse
import json
import logging
import os
from pathlib import Path

## local modules
import constants as const
from modules import AreaOfInterest as aoi
from modules import MapillaryInterface as mi
from modules import Models as md
from modules import SurfaceDatabase as sd


def run_pipeline(args, root_path):
    cg, credentials = get_config(args.configfile, root_path)
    db, aoi, mi, md = setup_pipeline(cg, credentials)

    has_img_metadata = db.table_exists(f"{aoi.name}_img_metadata")
    if (not has_img_metadata) | args.query_images:
        logging.info(f"query img metadata and store in database {db.dbname}")
        aoi.get_and_write_img_metadata(mi, db)
    else:
        logging.info(
            "Configured to not query new image metadata. Skip image metadata download."
        )

    has_road_seg_table = db.table_exists(f"{aoi.name}_way_selection")
    if not has_road_seg_table or args.recreate_roads:
        logging.info("Create road segments in bounding box.")
        query_path = (
            const.SQL_WAY_SELECTION if db.osm_region else const.SQL_WAY_SELECTION_CUSTOM
        )
        db.execute_sql_query(query_path, aoi.query_params)
    else:
        logging.info("Previous road segments found. Skip road segment creation.")

    logging.info(f"Cut lines into subsegments of length {aoi.segment_length}.")
    db.execute_sql_query(const.SQL_SEGMENT_WAYS, aoi.query_params)

    logging.info("Match images to subsegments.")
    db.execute_sql_query(const.SQL_MATCH_IMG_ROADS, aoi.query_params)

    ##### classify images
    if args.query_images:
        logging.info("Classify images")
        aoi.classify_images(mi, db, md)
    else:
        logging.info(
            "Only use existing image classifications. Skip classification step."
        )

    db.execute_sql_query(const.SQL_PREPARE_PARTITIONS, aoi.query_params)
    if db.osm_region is not None:  # is OSM file?
        logging.info("Create partitions for each road type of a road segment.")
        db.execute_sql_query(const.SQL_SEPARATE_ROAD_TYPES, aoi.query_params)
    else:
        db.execute_sql_query(const.SQL_SEPARATE_NULL_ROAD_TYPES, aoi.query_params)
        logging.info(
            "Custom road network - create cycleway and sidewalk partitions for all null valued roads."
        )

    logging.info("Aggregate by road segment.")
    # split into three scripts for faster execution
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(1), aoi.query_params)
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(2), aoi.query_params)
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(3), aoi.query_params)

    if args.export_results:
        logging.info("Export results to shapefiles.")
        results_to_files(aoi, db)
    else:
        logging.info("Skip exporting results to shapefiles.")


def get_config(configfile, root_path):

    global_config_path = root_path / "configs" / "00_global_config.json"
    credentials_path = root_path / "configs" / "02_credentials.json"
    if configfile is not None:
        config_path = root_path / "configs" / f"{configfile}.json"
    else:
        config_path = root_path / "configs" / "dresden_small.json"

    # Read and parse the JSON configuration file
    with open(global_config_path, "r") as config_file:
        global_cg = json.load(config_file)
    with open(config_path, "r") as config_file:
        cg = json.load(config_file)
    cg = {**global_cg, **cg}
    with open(credentials_path, "r") as cred_file:
        credentials = json.load(cred_file)

    return cg, credentials


def setup_pipeline(cg, credentials):
    mi_params = {
        key: value
        for key, value in {**cg, **credentials}.items()
        if key in ["mapillary_token", "parallel", "parallel_batch_size"]
    }
    mapillary_interface = mi.MapillaryInterface(**mi_params)

    area_of_interest = aoi.AreaOfInterest(cg)
    model_interface = md.ModelInterface(cg)

    # only pass on provided parameters - for missing values set SurfaceDatabase defaults
    sd_params = {
        key: value
        for key, value in {**cg, **credentials}.items()
        if key
        in [
            "dbname",
            "dbuser",
            "dbpassword",
            "dbhost",
            "dbport",
            "pbf_folder",
            "osm_region",
            "road_network_path",
            "sql_custom_way_prep",
        ]
    }
    surface_database = sd.SurfaceDatabase(**sd_params)

    return surface_database, area_of_interest, mapillary_interface, model_interface


def results_to_files(area_of_interest, surface_database):
    # write results to shapefile
    output_folder = root_path / "data" / "output"
    os.makedirs(output_folder, exist_ok=True)
    run = f"_{area_of_interest.run}" if area_of_interest.run else ""
    output_file = output_folder / f"{area_of_interest.name}{run}_surfaceai.shp"
    logging.info(f"Write results to shapefile {output_file}.")

    # surface_database.table_to_shapefile(
    #     f"{area_of_interest.name}_group_predictions", output_file
    # )

    output_file = output_folder / f"{area_of_interest.name}{run}_img_metadata.shp"
    area_of_interest.imgs_to_shapefile(surface_database, output_file)

    output_file = output_folder / f"{area_of_interest.name}{run}_surfaceai_gt.shp"
    area_of_interest.road_network_with_osm_groundtruth(surface_database, output_file)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

    # load config
    parser = argparse.ArgumentParser(prog="surfaceAI")
    parser.add_argument("-c", "--configfile")
    parser.add_argument(
        "--recreate_roads", action=argparse.BooleanOptionalAction, default=False
    )
    parser.add_argument(
        "--query_images", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument(
        "--export_results", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args()

    root_path = Path(os.path.abspath(__file__)).parent.parent

    run_pipeline(args, root_path)

    # surface_database.remove_temp_tables(area_of_interest.name)
