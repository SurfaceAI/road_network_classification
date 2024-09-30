import os
import sys
import subprocess
from pathlib import Path
import json

import pandas as pd

import logging 

## local modules
import utils 
import config
import src.constants as const
from modules import SurfaceDatabase as sd, MapillaryInterface as mi, AreaOfInterest as aoi

root_path = str(Path(os.path.abspath(__file__)).parent.parent)
sys.path.append(root_path)
import database_credentials as db


# def img_selection(dbname, name, n_per_segment):
#     logging.info("select images")

#     query_file = os.path.join(root_path, const.SQL_IMG_SELECTION)
#     utils.execute_sql_query(dbname, query_file, 
#                     {"table_name": f"{name}_img_metadata", 
#                     "table_name_point_selection": f"{name}_point_selection",
#                     "n_per_segment": n_per_segment})


def aggregate_by_road_segment(dbname, name, point_table, min_road_length, segments_per_group, additional_id_column=""):
    logging.info("aggregate by road segment")
    additional_id_column = f"{additional_id_column}," if additional_id_column != "" else "" # add comma
    grouping_ids = f"{additional_id_column}id,part_id,group_num"
    additional_id_column = f"ways.{additional_id_column}" if additional_id_column != "" else ""
    params = {      "name": name,
                    "additional_id_column": additional_id_column,
                    "grouping_ids": grouping_ids,
                    "table_name_point_selection": point_table,
                    "segments_per_group" : segments_per_group,
                    "min_road_length": min_road_length}

    query_file = const.SQL_AGGREGATE_ON_ROADS.format(1)
    query_file = os.path.join(root_path, query_file)
    utils.execute_sql_query(dbname, query_file, params)

    query_file = const.SQL_AGGREGATE_ON_ROADS.format(2)
    query_file = os.path.join(root_path, query_file)
    utils.execute_sql_query(dbname, query_file, params)

    query_file = const.SQL_AGGREGATE_ON_ROADS.format(3)
    query_file = os.path.join(root_path, query_file)
    utils.execute_sql_query(dbname, query_file, params)

    logging.info("aggregation complete")

def roadtype_separation(dbname, name, custom_road_type_separation):
    query_file = const.SQL_ASSIGN_ROAD_TYPES if not custom_road_type_separation else custom_road_type_separation
    if query_file != None:
        query_file = os.path.join(root_path, query_file)
        logging.info("create partitions for each road type of a geometry in a separate table")

        utils.execute_sql_query(dbname, query_file, 
                        {"name": name,
                         "table_name_img_selection": name})
        logging.info("road type separation complete")
    else:
        logging.info("no road type separation needed")
        



if __name__ == "__main__":

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    global_config_path = os.path.join(root_path, "configs", "00_global_config.json")
    if len(sys.argv) > 1:
        config_path = os.path.join(root_path, "configs", sys.argv[1])
    else:
        cg = config.dresden_small
        config_path = os.path.join(root_path, "configs", "dresden_small.json")


    # Read and parse the JSON configuration file
    with open(global_config_path, 'r') as config_file:
        global_cg = json.load(config_file)
    
    with open(config_path, 'r') as config_file:
        cg = json.load(config_file)
        cg = {**global_cg, 
              **cg}
        
    # TODO: verify config inputs

    data_path = os.path.join(root_path, cg["data_root"], cg["name"])
    dbname = getattr(db, cg["database"])
    mapillary_token = utils.get_access_token(cg["mapillary_token_path"], parallel = cg["parallel"], parallel_batch_size=cg["parallel_batch_size"])

    mapillary_interface = mi.MapillaryInterface(mapillary_token)    

    area_of_interest = aoi.AreaOfInterest(cg)
    
    ##### ---- setup database if it does not exist ---- ######
    surface_database = sd.SurfaceDatabase(dbname, cg["pbf_path"], root_path)
    
    #### ---- compute surface values for area of interest ---- ######
    surface_database.process_area_of_interest(area_of_interest, mapillary_interface)

    # needed to create csv for server
    #utils.img_ids_to_csv(data_path, db_table=f"{cg["name"]}", file_name = cg["img_selection_csv_path"])


    #### ---- aggregation algorithm ---- ####
    # create table with road partitions (i.e., each road type for each geoemtry)
    custom_road_type_separation = False if "custom_road_type_separation" not in cg.keys() else cg["custom_road_type_separation"]
    roadtype_separation(dbname, cg["name"], custom_road_type_separation=custom_road_type_separation)

    aggregate_by_road_segment(dbname, cg["name"], point_table=point_table, 
                            min_road_length=cg["min_road_length"], segments_per_group=cg["segments_per_group"], 
                            additional_id_column=additional_id_column)

    # write result to shape file
    output_file = os.path.join(data_path, cg["run"], f"{cg["name"]}_pred.shp")
    subprocess.run(f'pgsql2shp -f "{output_file}" {dbname} "select * from {cg["name"]}_group_predictions"', shell = True, executable="/bin/bash")
    logging.info(f"result shp file written to {output_file}")