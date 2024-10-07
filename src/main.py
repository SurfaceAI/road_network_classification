import os
import sys
from pathlib import Path
import json
import logging

# sys.path.append(root_path)

## local modules
from modules import (
    SurfaceDatabase as sd,
    MapillaryInterface as mi,
    AreaOfInterest as aoi,
)


def process_area_of_interest(db, aoi, mi):
    logging.info(f"create img table {aoi.name} in database {db.dbname}")
    mi.query_metadata(aoi)
    db.execute_sql_query(aoi.query_files["add_img_metadata_table"], aoi.query_params)
    aoi.remove_img_metadata_file()

    logging.info("create linestrings in bounding box")
    db.execute_sql_query(aoi.query_files["way_selection"], aoi.query_params)

    logging.info(f"cut lines into segments of length {aoi.segment_length}")
    db.execute_sql_query(aoi.query_files["segment_ways"], aoi.query_params)

    logging.info("match imgs to road linestrings")
    db.execute_sql_query(aoi.query_files["match_img_roads"], aoi.query_params)

    ##### classify images
    # TODO: parallelize download and classification step?
    # mi.download_imgs_from_table(
    #                 os.path.join(aoi.data_path, aoi.run, "imgs"),
    #                 aoi.img_size,
    #                 database=self,
    #                 db_table=f"{aoi.name}_img_metadata")

    # TODO: include classification model into pipeline
    # self.classify()

    logging.info("add classification results to db")
    aoi.format_pred_files()
    db.execute_sql_query(aoi.query_files["add_surface_pred_results"], aoi.query_params)
    db.execute_sql_query(
        aoi.query_files["add_road_type_pred_results"], aoi.query_params
    )

    if aoi.query_files["roadtype_separation"] != None:
        logging.info(
            "create partitions for each road type of a geometry in a separate table"
        )
        db.execute_sql_query(aoi.query_files["roadtype_separation"], aoi.query_params)
    else:
        logging.info("no road type separation needed")

    logging.info("aggregate by road segment")
    db.execute_sql_query(aoi.query_files["aggregate_on_roads_1"], aoi.query_params)
    db.execute_sql_query(aoi.query_files["aggregate_on_roads_2"], aoi.query_params)
    db.execute_sql_query(aoi.query_files["aggregate_on_roads_3"], aoi.query_params)

    # write results to shapefile
    output_file = os.path.join(
        aoi.data_path,
        aoi.run,
        f"{aoi.name}_pred.shp",
    )
    db.table_to_shapefile(f"{aoi.name}_group_predictions", output_file)


if __name__ == "__main__":
    logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

    root_path = str(Path(os.path.abspath(__file__)).parent.parent)
    global_config_path = os.path.join(root_path, "configs", "00_global_config.json")
    credentials_path = os.path.join(root_path, "configs", "01_credentials.json")
    if len(sys.argv) > 1:
        config_path = os.path.join(root_path, "configs", sys.argv[1])
    else:
        config_path = os.path.join(root_path, "configs", "dresden_small.json")

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
        parallel=cg["parallel"],
        parallel_batch_size=cg["parallel_batch_size"],
    )

    # TODO: only append root_path if data_root is a relative path
    cg["data_root"] = os.path.join(root_path, cg["data_root"])
    area_of_interest = aoi.AreaOfInterest(cg)

    # TODO: only append root_path if pbf_path is a relative path
    cg["pbf_path"] = os.path.join(root_path, cg["pbf_path"])
    surface_database = sd.SurfaceDatabase(
        credentials[cg["dbname"]],
        credentials[cg["dbuser"]],
        credentials[cg["dbhost"]],
        credentials[cg["dbpassword"]],
        cg["pbf_path"],
    )

    process_area_of_interest(surface_database, area_of_interest, mapillary_interface)
