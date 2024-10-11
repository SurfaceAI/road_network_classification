import argparse
import json
import logging
import os
import sys
from pathlib import Path

from modules import (
    AreaOfInterest as aoi,
)
from modules import (
    MapillaryInterface as mi,
)

## local modules
from modules import (
    SurfaceDatabase as sd,
    MapillaryInterface as mi,
    AreaOfInterest as aoi,
    Models as md,
)

import constants as const


def process_area_of_interest(db, aoi, mi, md):
    logging.info(f"query img metadata and store in database {db.dbname}")
    # aoi.get_and_write_img_metadata(mi, db)

    # logging.info("create linestrings in bounding box")
    # # TODO: speed up?
    # db.execute_sql_query(aoi.custom_query_files["way_selection"], aoi.query_params)

    # logging.info(f"cut lines into segments of length {aoi.segment_length}")
    # db.execute_sql_query(const.SQL_SEGMENT_WAYS, aoi.query_params)

    # logging.info("match images to road segments")
    # db.execute_sql_query(const.SQL_MATCH_IMG_ROADS, aoi.query_params)

    ##### classify images
    # TODO: include classification model into pipeline
    logging.info("classify images")
    aoi.classify_images(mapillary_interface, surface_database, model_interface)

    if aoi.custom_query_files["roadtype_separation"] is not None:
        logging.info(
            "create partitions for each road type of a road segment"
        )
        db.execute_sql_query(aoi.custom_query_files["roadtype_separation"], aoi.query_params)
    else:
        logging.info("no road type separation needed")

    logging.info("aggregate by road segment")
    # split into three scripts for faster execution
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(1), aoi.query_params)
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(2), aoi.query_params)
    db.execute_sql_query(str(const.SQL_AGGREGATE_ON_ROADS).format(3), aoi.query_params)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

    # load config
    parser = argparse.ArgumentParser(prog="surfaceAI")
    parser.add_argument('-c', '--configfile')
    args = parser.parse_args()

    root_path = Path(os.path.abspath(__file__)).parent.parent
    global_config_path = root_path / "configs"/ "00_global_config.json"
    credentials_path = root_path / "configs" / "01_credentials.json"
    if args.configfile:
        config_path = root_path / "configs" / f"{args.configfile}.json"
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

    mapillary_interface = mi.MapillaryInterface(
        mapillary_token=credentials["mapillary_token"],
        parallel=cg.get("parallel"),
        parallel_batch_size=cg.get("parallel_batch_size"),
    )

    area_of_interest = aoi.AreaOfInterest(cg)
    model_interface = md.ModelInterface(cg)

    # TODO: only append root_path if pbf_path is a relative path
    cg["pbf_path"] = root_path / cg.get("pbf_path")
    surface_database = sd.SurfaceDatabase(
        credentials[cg.get("dbname")],
        credentials[cg.get("dbuser")],
        credentials[cg.get("dbhost")],
        credentials[cg.get("dbpassword")],
        cg.get("pbf_path"),
    )

    process_area_of_interest(surface_database, area_of_interest, mapillary_interface, model_interface)

    os.makedirs(root_path / "data", exist_ok=True)
    # write results to shapefile
    output_file = root_path / "data" / f"{area_of_interest.name}_{area_of_interest.run}_pred.shp"
    surface_database.table_to_shapefile(
        f"{area_of_interest.name}_group_predictions", output_file
    )
