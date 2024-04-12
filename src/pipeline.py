import os
import sys

import pandas as pd

import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

import utils 

sys.path.append("./")
import database_credentials as db

### Step 1: define bounding box ###
minLon=13.4029690
minLat=52.4957929
maxLon=13.4085637
maxLat=52.4991356
name = "s1"

minLon=13.4097172387
minLat=52.49105842
maxLon=13.4207674991
maxLat=52.4954385756
name = "s2"

# ### Step 2: download mapillary images ###

step_1 = True
step_2 = False


if step_1:
    # # get all relevant tile ids
    tiles = utils.write_tiles_within_boundary("tiles.csv", minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)

    # # download img metadata
    utils.query_and_write_img_metadata(pd.DataFrame(tiles), f"data/{name}_img_metadata.csv", 
                                    minLon=minLon, minLat=minLat, maxLon=maxLon, maxLat=maxLat)

    ## TODO:
    # filter first - then download filtered imgs
    # filter by segment


    # # download imgs
    utils.download_images(f"data/{name}_img_metadata.csv", f"data/{name}")

### Step 3: classify images ###
# use classification_model code
# result: test_sample-aggregation_sample-20240305_173146.csv

if step_2:
    ### Ste 4: aggregate on road segments ###
    table_name = f"{name}_aggregate"
    table_name_snapped = f"{name}_aggregate_snapped"
    aggregate_sample_path = f"data/{name}_img_metadata_with_label.csv"
    output_path = f"data/{name}_aggregate_snapped.csv"

    # Connect to your PostgreSQL database
    conn = psycopg2.connect(
        dbname=db.database,
        user=db.user,
        host=db.host,
    )

    temp_path = "data/temp.csv"
    image_selection = pd.read_csv(aggregate_sample_path)
    image_selection.drop("date", axis=1).to_csv(temp_path, index=False)

    absolute_path = os.path.join(os.getcwd(), temp_path)
    output_path = os.path.join(os.getcwd(), output_path)

    # create table with sample data points
    with open("src/sample_to_sql.sql", "r") as file:
        query_create_table = file.read()

    with open("src/snap_points_on_roadseg.sql", "r") as file:
        query_snap = file.read()


    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(sql.SQL(query_create_table.format(table_name=table_name, absolute_path=absolute_path)))
        conn.commit()

        os.remove(absolute_path)

    # snap points on road segments
        cursor.execute(sql.SQL(
                    query_snap.format(
                        bbox0=minLon, bbox1=minLat, bbox2=maxLon, bbox3=maxLat,table_name_snapped=table_name_snapped
                    )
                ))
        conn.commit()

        cursor.execute(sql.SQL(f"copy (select * from {table_name_snapped}) TO '{output_path}' DELIMITER ',' CSV HEADER;"))
        conn.commit()

    conn.close()




    # clean table bc trailing whitespace is stored during export # TODO: better way while exporting from SQL?
    df = pd.read_csv(output_path)
    for column in df.columns:
        if (df[column].dtype == "str") | (df[column].dtype == "object"):
            df[column] = df[column].str.strip()
    df.to_csv(output_path, index=False)








# get all relevant img ids
# with open(token_path, "r") as file:
#     # access_token = tokenfile.readlines()
#     access_tokens = [line.strip() for line in file.readlines()]
# current_token = 0

# response = requests.get(
#     f"https://graph.mapillary.com/images?access_token=$TOKEN&fields=id&bbox={minLon},{minLat},{maxLon},{maxLat}",
#     params={"access_token": access_tokens[current_token]}
#     )

# data = json.loads(response.content.decode('utf-8'))

# output = []
# img_ids = [id["id"] for id in data["data"]]


# # a feature is a point/image
# # TODO: can this be speed up?
# for feature in data["data"]:
#     output.append(
#         [
#             feature["properties"]["id"],
#             feature["properties"]["sequence_id"],
#             feature["properties"]["captured_at"],
#             feature["properties"]["compass_angle"],
#             feature["properties"]["is_pano"],
#             feature["properties"]["creator_id"],
#             feature["geometry"]["coordinates"][0],
#             feature["geometry"]["coordinates"][1],
#         ]
#     )
