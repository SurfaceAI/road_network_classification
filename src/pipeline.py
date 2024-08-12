import os
import sys
import subprocess
from pathlib import Path

import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

import logging 

import utils 
import config
import src.constants as const

sys.path.append(str(Path(os.path.abspath(__file__)).parent.parent))

import database_credentials as db


def setup_database(dbname, pbf_path):
        # TODO: custom database
        # does the db already exist?
        res = subprocess.run(r"psql -lqt | cut -d \| -f 1 | grep -w " + dbname, shell = True, executable="/bin/bash")
        if res.returncode == 0:
            logging.info(f"database already exists. Skip DB creation step.")
        else:
            logging.info("setup database. Depending on the pbf_file size this might take a while")
            osmosis_scheme_file = os.path.join(Path(os.path.dirname(__file__)).parent, 'pgsnapshot_schema_0.6.sql')
            subprocess.run(f"createdb {dbname}", shell = True, executable="/bin/bash")
            subprocess.run(f"psql  -d {dbname} -c 'CREATE EXTENSION postgis;'", shell = True, executable="/bin/bash")
            subprocess.run(f"psql  -d {dbname} -c 'CREATE EXTENSION hstore;'", shell = True, executable="/bin/bash")
            subprocess.run(f"psql -d {dbname} -f {osmosis_scheme_file}", shell = True, executable="/bin/bash")
            subprocess.run(f"""osmosis --read-pbf {pbf_path} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={dbname}""", shell = True, executable="/bin/bash")
            logging.info("database setup complete")


def query_img_meta_in_bbox(data_path, minLon, minLat, maxLon, maxLat, name, userid=None, no_pano=True):

    logging.info("Querying img metadata in bbox")
    # ### download mapillary img metadata ###
    # # get all relevant tile ids
    tiles = utils.write_tiles_within_boundary("tiles.csv", minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)
    # # download img metadata
    output_path = os.path.join(data_path, f"{name}_img_metadata.csv")
    os.makedirs(data_path, exist_ok=True)
    utils.query_and_write_img_metadata(pd.DataFrame(tiles), output_path, 
                                    minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat, 
                                    userid=userid, no_pano=no_pano)

    logging.info(f"img metadata written to {output_path}")
    return output_path


def img_to_db(dbname, data_path, name):
    logging.info(f"create img table {name} in database")

    aggregate_sample_path = os.path.join(data_path, f"{name}_img_metadata.csv")
    temp_path = os.path.join(os.getcwd(), data_path, "temp.csv")
    image_selection = pd.read_csv(aggregate_sample_path)
    #image_selection.drop("date", axis=1)
    image_selection.to_csv(temp_path, index=False)
    
    utils.execute_sql_query(dbname, const.SQL_IMGS_TO_DB, 
                      {"table_name": f"{name}", 
                       "absolute_path": temp_path})
    os.remove(temp_path)


def prepare_line_segments(dbname, name, minLon, minLat, maxLon, maxLat, 
                          segment_length=20, custom_sql_way_selection=False,
                          custom_attrs={}):

    logging.info(f"create linestrings in bounding box")
    query_file = const.SQL_WAY_SELECTION if not custom_sql_way_selection else custom_sql_way_selection

    utils.execute_sql_query(dbname, query_file, 
                        {"bbox0": minLon, "bbox1": minLat, "bbox2": maxLon, "bbox3": maxLat,
                        "table_name_way_selection": f"{name}_way_selection", **custom_attrs
                        })

    logging.info(f"cut lines into segments of length {segment_length}")
    utils.execute_sql_query(dbname, const.SQL_SEGMENT_WAYS, 
                      {"table_name_way_selection": f"{name}_way_selection",
                       "segment_length": segment_length,
                       })
    # export table to csv
    #    cursor.execute(sql.SQL(f"copy (select * from {table_name_snapped}) TO '{output_path}' DELIMITER ',' CSV HEADER;"))
    #    conn.commit()

def img_selection(dbname, name, n_per_segment):
    print("select images")

    utils.execute_sql_query(dbname, const.SQL_IMG_SELECTION, 
                    {"table_name": f"{name}", 
                    "table_name_point_selection": f"{name}_point_selection",
                    "n_per_segment": n_per_segment})


def match_img_to_roads(dbname, name, crs=None):
    logging.info("match imgs to road linestrings")

    crs = 3035 if crs is None else crs

    # this also takes a while: for Dresden (0.5 Mio imgs) around xx
    utils.execute_sql_query(dbname, const.SQL_MATCH_IMG_ROADS, 
                      {"table_name": f"{name}", 
                       "table_name_point_selection": f"{name}_point_selection",
                       "crs": crs})
    logging.info("matching complete")
    

def img_download(data_path, run, dbname, img_size, dest_folder_name = "imgs", csv_path = None, db_table = None, 
                 parallel=True, img_id_col=False):

    if csv_path:
        img_id_col = 1 if not img_id_col else img_id_col
        img_ids = utils.img_ids_from_csv(csv_path, img_id_col=img_id_col)
    elif db_table:
        img_ids = utils.img_ids_from_dbtable(db_table, dbname)


    # only download images that are not present yet in download folder
    download_folder = os.path.join(data_path, run, dest_folder_name)
    if os.path.exists(download_folder):
        imgs_in_download_folder =  os.listdir(download_folder)
        imgIDs_in_download_folder = [img_id.split(".")[0] for img_id in imgs_in_download_folder]
        img_ids = list(set(img_ids) - set(imgIDs_in_download_folder))

    logging.info(f"Downloading {len(img_ids)} images")
    utils.download_images(img_ids, download_folder, img_size=img_size, 
                          parallel=parallel)

### classify images ###
# use classification_model code
def img_classification(dbname, data_path, name, pred_path, run, pano=False, road_scenery_path=False):
    print("add classification results to db")
    pred = utils.format_predictions(pd.read_csv(pred_path, dtype={"Image": str}), pano=pano)
    csv_path = os.path.join(os.getcwd(),data_path,  name, run, "classification_results.csv")
    pred.to_csv(csv_path, index=False)

    query_file = const.SQL_JOIN_MODEL_PRED_DIR if pano else const.SQL_JOIN_MODEL_PRED

    utils.execute_sql_query(dbname, query_file, 
                    {"table_name_point_selection": f"{name}_point_selection",
                    "csv_path": csv_path,
                    "pano": pano})

    if road_scenery_path:
        pred_scene = utils.format_scenery_predictions(pd.read_csv(road_scenery_path, dtype={"Image": str}), pano=pano)
        csv_path_scenery = os.path.join(os.getcwd(),data_path,  name, run, "scenery_class_results.csv")
        pred_scene.to_csv(csv_path_scenery, index=False)

        utils.execute_sql_query(dbname, const.SQL_JOIN_SCENERY_PRED, 
                    {"table_name_point_selection": f"{name}_point_selection",
                    "csv_path": csv_path_scenery,
                    "pano": pano})


def aggregate_by_road_segment(dbname, name):
    logging.info("aggregate by road segment")

    utils.execute_sql_query(dbname, const.SQL_AGGREGATE_ON_ROADS, 
                    {"table_name_point_selection": f"{name}_point_selection",
                    "table_name_way_selection": f"{name}_way_selection"})



def img_ids_to_csv(data_path, db_table, file_name):
    ids = utils.img_ids_from_dbtable(db_table, dbname)
    pd.DataFrame({"img_id" : ids}).to_csv(os.path.join(data_path, file_name), index=False)



def roadtype_separation(dbname, name, data_path, custom_road_type_separation):
    logging.info("assign road_type and create individual geometries if not present yet")

    query_file = const.SQL_ASSIGN_ROAD_TYPES if not custom_road_type_separation else custom_road_type_separation
    utils.execute_sql_query(dbname, query_file, 
                    {"table_name_way_selection": f"{name}_way_selection"})
    
    output_file = os.path.join(data_path, f"{name}.shp")
    subprocess.run(f'pgsql2shp -f "{output_file}" {dbname} "select * from {name}_way_selection"', shell = True, executable="/bin/bash")
    logging.info(f"result shp file written to {output_file}")


if __name__ == "__main__":
    
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    cg = config.dresden
    data_path = os.path.join(cg["data_root"], cg["name"])
    
    dbname = getattr(db, cg["database"])

    # setup database if it does not exist
    # setup_database(dbname, pbf_path=cg["pbf_path"])

    ###### ---- get image metadata ---- ######
    # TODO: include pano images?
    # TODO: write directly into DB (without csv?)
    # query_img_meta_in_bbox(data_path, cg["minLon"], cg["minLat"], cg["maxLon"], cg["maxLat"], cg["name"])
    # img_to_db(dbname, data_path, cg["name"])
    
    ##### ---- prepare line segments ---- ######
    custom_sql_way_selection = False if "custom_sql_way_selection" not in cg.keys() else cg["custom_sql_way_selection"]
    custom_attrs = {} if "custom_attrs" not in cg.keys() else cg["custom_attrs"]
    # TODO: This step takes quite long for OSM - possible to speed up?
    # prepare_line_segments(dbname, cg["name"], cg["minLon"], cg["minLat"], cg["maxLon"], cg["maxLat"], 
    #             custom_sql_way_selection = custom_sql_way_selection, custom_attrs=custom_attrs)
    
    ##### ---- match images to road segments ---- ######
    crs = None if "crs" not in cg.keys() else cg["crs"]
    match_img_to_roads(dbname, cg["name"], crs=crs)
    
    # limit number of images per road segment?
    if cg["n_per_segment"]:
        img_selection(dbname, cg["name"], cg["n_per_segment"])
        download_from = f'{cg["name"]}_point_selection'
    else:
        download_from = cg["name"]

    # needed to create csv for server
    # img_ids_to_csv(data_path, db_table=f"{cg["name"]}", file_name = cg["img_selection_csv_path"])
    
    ##### ---- download images ---- ######
    # img_download(data_path, cg["run"], dbname, db_table=download_from, img_size=cg["img_size"])
    
    #### ---- add classification information ---- ####
    # TODO: best way to integrate classification model?
    #img_classification(dbname, cg["data_root"], cg["name"], cg["pred_path"], run=cg["run"],
    #            pano=False, road_scenery_path=cg["road_scenery_pred_path"])
    
    #### ---- aggregation algorithm ---- ####
    #aggregate_by_road_segment(dbname, cg["name"])

    # split geoms if multiple road types are represented by one
    # custom_road_type_separation = False if "custom_road_type_separation" not in cg.keys() else cg["custom_road_type_separation"]
    # roadtype_separation(dbname, cg["name"], data_path, custom_road_type_separation=custom_road_type_separation)

