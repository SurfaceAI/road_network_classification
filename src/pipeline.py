import os
import sys

import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

import utils 
import config
import constants as const

sys.path.append("./")
import database_credentials as db


def query_img_meta_in_bbox(data_path, minLon, minLat, maxLon, maxLat, name, userid=None, no_pano=True):
    # ### download mapillary img metadata ###
    # # get all relevant tile ids
    tiles = utils.write_tiles_within_boundary("tiles.csv", minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)
    # # download img metadata
    output_path = os.path.join(data_path, f"{name}_img_metadata.csv")
    os.makedirs(data_path, exist_ok=True)
    utils.query_and_write_img_metadata(pd.DataFrame(tiles), output_path, 
                                    minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat, 
                                    userid=userid, no_pano=no_pano)

    return output_path





def img_to_db(dbname, data_path, name):
    print("sql create img table")

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
                          segment_length=20, custom=False, 
                          custom_edge_geom_table=False, orig_way_id_name="id"):

    print("prepare lines")
    if not custom:
        query_file = const.SQL_LINE_SEGMENTS
        edge_table_name = ""
        
    else:
        query_file = const.SQL_LINE_SEGMENTS_CUST
        edge_table_name = custom_edge_geom_table

    utils.execute_sql_query(dbname, query_file, 
                      {"bbox0": minLon, "bbox1": minLat, "bbox2": maxLon, "bbox3": maxLat,
                       "table_name": f"{name}", 
                       "table_name_snapped": f"{name}_snapped",
                       "table_name_point_selection": f"{name}_point_selection",
                       "table_name_way_selection": f"{name}_way_selection",
                       "segment_length": segment_length,
                       "edge_table_name": edge_table_name,
                       "old_way_id_name": orig_way_id_name
                       })
    # export table to csv
    #    cursor.execute(sql.SQL(f"copy (select * from {table_name_snapped}) TO '{output_path}' DELIMITER ',' CSV HEADER;"))
    #    conn.commit()

def img_selection(dbname, name):
    print("select images")

    utils.execute_sql_query(dbname, const.SQL_IMG_SELECTION, 
                    {"table_name": f"{name}", 
                    "table_name_snapped": f"{name}_snapped",
                    "table_name_point_selection": f"{name}_point_selection"})


def match_img_to_roads(dbname, name, custom=False):
    print("match imgs to roads")

    query_file = const.SQL_MATCH_IMG_ROADS if not custom else const.SQL_MATCH_IMG_ROADS_CUST

    # TODO: do we actually need to snap or is ref. to closest geom enough?
    utils.execute_sql_query(dbname, query_file, 
                      {"table_name": f"{name}", 
                       "table_name_snapped": f"{name}_snapped",
                       "table_name_point_selection": f"{name}_point_selection"})

    # clean table bc trailing whitespace is stored during export 
    # TODO: better way while exporting from SQL?
    # df = pd.read_csv(output_path)
    # for column in df.columns:
    #     if (df[column].dtype == "str") | (df[column].dtype == "object"):
    #         df[column] = df[column].str.strip()
    # df.to_csv(output_path, index=False)



def img_download(data_path,run, img_size, dest_folder_name = "imgs", csv_path = None, db_table = None, 
                 parallel=True, custom=False):
    dbname = db.database if not custom else getattr(db, custom)

    if csv_path:
        img_ids = utils.img_ids_from_csv(csv_path)
    elif db_table:
        img_ids = utils.img_ids_from_dbtable(db_table, dbname)

    print("Downloading images")
    utils.download_images(img_ids, os.path.join(data_path, run, dest_folder_name), img_size=img_size, 
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
                    {"table_name_snapped": f"{name}_snapped",
                    "csv_path": csv_path,
                    "pano": pano})

    if road_scenery_path:
        pred_scene = utils.format_scenery_predictions(pd.read_csv(road_scenery_path, dtype={"Image": str}), pano=pano)
        csv_path_scenery = os.path.join(os.getcwd(),data_path,  name, run, "scenery_class_results.csv")
        pred_scene.to_csv(csv_path_scenery, index=False)

        utils.execute_sql_query(dbname, const.SQL_JOIN_SCENERY_PRED, 
                    {"table_name_snapped": f"{name}_snapped",
                    "csv_path": csv_path_scenery,
                    "pano": pano})


def aggregate_by_road_segment(dbname, name):
    print("aggregate by road segment")

    utils.execute_sql_query(dbname, const.SQL_AGGREGATE_ON_ROADS, 
                    {"table_name_point_selection": f"{name}_snapped",
                    "table_name_way_selection": f"{name}_way_selection"})



def roadtype_seperation():
        utils.execute_sql_query(dbname, "src/sql/roadtypes.sql")


if __name__ == "__main__":

    cg = config.berlin_prio_vset
    data_path = os.path.join(cg["data_root"], cg["name"])
    
    
    # query_img_meta_in_bbox(data_path, cg["minLon"], cg["minLat"], cg["maxLon"], cg["maxLat"], cg["name"])
    
    
    dbname = db.database if not cg["database"] else getattr(db, cg["database"])
    
    #img_to_db(dbname, data_path, cg["name"])
    #prepare_line_segments(dbname, cg["name"], cg["minLon"], cg["minLat"], cg["maxLon"], cg["maxLat"], 
    #             custom=cg["database"], custom_edge_geom_table=cg["custom_edge_geom_table"], 
    #             orig_way_id_name=cg["orig_way_id_name"])
    # match_img_to_roads(cg["name"], custom=cg["database"])
    img_selection(dbname, cg["name"])
    # img_download(data_path, cg["run"], db_table=f"{cg["name"]}_point_selection",custom=cg["database"], img_size=cg["img_size"])
    
    # TODO: crop pano images to perspective images
    # img_classification(dbname, cg["data_root"], cg["name"], cg["pred_path"], run=cg["run"],
    #                pano=False, road_scenery_path=cg["road_scenery_pred_path"])
    # aggregate_by_road_segment(dbname, cg["name"])

    # pgsql2shp -f "weser_aue_ways_pred.shp" osmGermany "select * from weser_aue_way_selection"
    # pgsql2shp -f "berlin_ways_pred.shp" berlinPrio "select * from berlin_prio_vset_way_selection"

