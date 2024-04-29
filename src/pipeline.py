import os
import sys

import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

import utils 

sys.path.append("./")
import database_credentials as db


def img_sample(data_path, minLon, minLat, maxLon, maxLat, name, userid=None, no_pano=True):
    # ### download mapillary img metadata ###
    # # get all relevant tile ids
    tiles = utils.write_tiles_within_boundary("tiles.csv", minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)
    # # download img metadata
    output_path = os.path.join(data_path, f"{name}_img_metadata.csv")
    utils.query_and_write_img_metadata(pd.DataFrame(tiles), output_path, 
                                    minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat, 
                                    userid=userid, no_pano=no_pano)

    return output_path


def img_selection(data_path, minLon, minLat, maxLon, maxLat, name):
    ## TODO:
    # filter first - then download filtered imgs
    # filter by segment

    # TODO: filter according to segmentation: include images on road and pedestrian
    ### aggregate on road segments ###
    aggregate_sample_path = os.path.join(data_path, f"{name}_img_metadata.csv")
    output_path = os.path.join(data_path, f"{name}_aggregate_snapped.csv")

    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=db.database,
        user=db.user,
        host=db.host,
    )

    temp_path = os.path.join(data_path, "temp.csv")
    image_selection = pd.read_csv(aggregate_sample_path)
    #image_selection.drop("date", axis=1)
    image_selection.to_csv(temp_path, index=False)

    absolute_path = os.path.join(os.getcwd(), temp_path)
    output_path = os.path.join(os.getcwd(), output_path)

    # create table with sample data points
    with open("src/sql/01_sample_to_sql.sql", "r") as file:
        query_create_table = file.read()

    with open("src/sql/02_intersect_img_ways.sql", "r") as file:
        query_snap = file.read()


    with conn.cursor(cursor_factory=DictCursor) as cursor:
        print("sql create table")
        cursor.execute(sql.SQL(query_create_table.format(table_name=f"{name}_aggregate", 
                                                         absolute_path=absolute_path)))
        conn.commit()

        os.remove(absolute_path)


    # snap points on road segments
    # TODO: do we actually need to snap or is ref. to closest geom enough?
        print("sql snap points")
        cursor.execute(sql.SQL(
                    query_snap.format(
                        bbox0=minLon, bbox1=minLat, bbox2=maxLon, bbox3=maxLat,
                        table_name=f"{name}_aggregate",
                        table_name_snapped=f"{name}_aggregate_snapped",
                        table_name_point_selection=f"{name}_point_selection"
                    )
                ))
        conn.commit()

    # export table to csv
    #    cursor.execute(sql.SQL(f"copy (select * from {table_name_snapped}) TO '{output_path}' DELIMITER ',' CSV HEADER;"))
    #    conn.commit()
    conn.close()


    # clean table bc trailing whitespace is stored during export 
    # TODO: better way while exporting from SQL?
    # df = pd.read_csv(output_path)
    # for column in df.columns:
    #     if (df[column].dtype == "str") | (df[column].dtype == "object"):
    #         df[column] = df[column].str.strip()
    # df.to_csv(output_path, index=False)




def img_download(data_path, csv_path = None, db_table = None):

    if csv_path:
        img_ids = utils.img_ids_from_csv(csv_path)
    elif db_table:
        img_ids = utils.img_ids_from_dbtable(db_table)

    utils.download_images(img_ids, os.path.join(data_path, "imgs"))

### classify images ###
# use classification_model code
def img_classification(data_path, name, pred_path):
    model_prediction = pd.read_csv(pred_path, dtype={"Image": str}) 
    # the prediction holds a value for each surface and a class probability. Only keep the highest prob.
    idx = model_prediction.groupby("Image")["Prediction"].idxmax()
    model_prediction = model_prediction.loc[idx]   
    model_prediction.drop("is_in_validation", axis=1, inplace=True)
    model_prediction["Image"] = model_prediction["Image"].str.split("_").str[0]

    model_prediction.to_csv(os.path.join(data_path, "classification_results.csv"), index=False)

    pred_path = os.path.join(os.getcwd(),data_path, "classification_results.csv")
    # update table with classification results
    conn = psycopg2.connect(
        dbname=db.database,
        user=db.user,
        host=db.host,
    )

    with open("src/sql/03_classification_res.sql", "r") as file:
        query_classification = file.read()

    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(sql.SQL(query_classification.format(
            table_name_point_selection=f"{name}_point_selection",
            csv_path = pred_path)))
        conn.commit()
    conn.close()


def aggregate_by_road_segment(name):
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=db.database,
        user=db.user,
        host=db.host,
    )

    with open("src/sql/04_label_segment.sql", "r") as file:
        query_label = file.read()

    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(sql.SQL(query_label.format(table_name_point_selection=f"{name}_point_selection")))
        conn.commit()
    conn.close()


def roadtype_seperation():
    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=db.database,
        user=db.user,
        host=db.host,
    )

    with open("src/sql/05_roadtypes.sql", "r") as file:
        query_geom = file.read()

    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(sql.SQL(query_geom))
        conn.commit()
    conn.close()


if __name__ == "__main__":

    data_path = "data"
    ### USER INPUT: define bounding box ###
    minLon=13.4029690
    minLat=52.4957929
    maxLon=13.4085637
    maxLat=52.4991356
    name = "s1"
    pred_path = "test_sample-aggregation_sample-20240305_173146.csv"

    # minLon=13.4097172387
    # minLat=52.49105842
    # maxLon=13.4207674991
    # maxLat=52.4954385756
    # name = "s2"
    # pred_path = "test_sample-s2-20240415_101331.csv"

    data_path = os.path.join(data_path, name)
    pred_path = os.path.join(data_path, pred_path)

    #img_sample(data_path, minLon, minLat, maxLon, maxLat, name)
    img_selection(data_path, minLon, minLat, maxLon, maxLat, name)
    #img_download(data_path, db_table=f"{name}_point_selection")
    img_classification(data_path, name, pred_path)
    aggregate_by_road_segment(name)
    #roadtype_seperation()
