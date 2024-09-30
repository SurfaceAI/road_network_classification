import os
import sys

import csv
import numpy as np
import mercantile
import requests
from vt2geojson.tools import vt_bytes_to_geojson
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from pathlib import Path
import logging
import pandas as pd

from PIL import Image
import equilib


import geopandas as gpd
#import matplotlib.pyplot as plt

root_path=str(Path(os.path.abspath(__file__)).parent.parent)
sys.path.append(root_path)

import database_credentials as db

import config
import constants as const

def get_access_token(token_path):
    token_path = os.path.join(root_path, token_path)
    with open(token_path, "r") as file:
        # access_token = tokenfile.readlines()
        access_tokens = [line.strip() for line in file.readlines()]
        return access_tokens[0]


def tile_center(xtile, ytile, zoom):
    """Return longitude,latitude centroid coordinates of mercantile tile
    Args:
        xtile (int): x tile coordinate
        ytile (int): y tile coordinate
        zoom (int): zoom level

    Returns:
        (float, float): A tuple of longitude, latitude.
    """
    upperleft = mercantile.ul(xtile, ytile, zoom)
    upperright = mercantile.ul(xtile + 1, ytile, zoom)
    lowerleft = mercantile.ul(xtile, ytile - 1, zoom)

    # not completely exact but good enough for our purposes
    lon = (upperleft.lng + upperright.lng) / 2
    lat = (upperleft.lat + lowerleft.lat) / 2
    return lon, lat






def img_ids_from_csv(csv_path, img_id_col=1):
    with open(
        csv_path, newline=""
    ) as csvfile:
        csvreader = csv.reader(csvfile)
        image_ids = [row[img_id_col] for row in csvreader][1:]
    return image_ids


def img_ids_from_dbtable(db_table, dbname):
    conn = psycopg2.connect(
        dbname=dbname,
        user=db.user,
        host=db.host,
    )

    with conn.cursor(cursor_factory=DictCursor) as cursor:
        img_ids = cursor.execute(sql.SQL(f"SELECT img_id FROM {db_table}"))
        img_ids = cursor.fetchall()
        img_ids = [img_id[0] for img_id in img_ids]
    conn.close()
    return img_ids


def img_ids_to_csv(dbname, data_path, db_table, file_name):
    ids = img_ids_from_dbtable(db_table, dbname)
    pd.DataFrame({"img_id" : ids}).to_csv(os.path.join(data_path, file_name), index=False)
    logging.info(f"img ids written to {os.path.join(data_path, file_name)}")



def clean_surface(surface):
    if surface in ["compacted", "gravel", "ground", "fine_gravel", "dirt", "grass", "earth", "sand"]:
        return "unpaved"
    elif surface in ["cobblestone", "unhewn_cobblestone"]:
        return "sett"
    elif surface in ["concrete:plates", "concrete:lanes"]:
        return "concrete"
    elif surface in ["grass_paver"]:
        return "paving_stones"
    else:
        return surface


def query_coords(img_id, mapillary_token):
    response = requests.get(
    const.MAPILLARY_GRAPH_URL.format(img_id),
    params={"access_token": mapillary_token,
            "fields" : "geometry"},
    )
    data = response.json()
    return data["geometry"]["coordinates"]

def query_creator_name(image_id):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format(image_id),
        params={
            "fields": "creator",
            "access_token": access_tokens[current_token],
        },
    )

    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)
        print(f"image_id: {image_id}")
        return False
    else:
        data = response.json()
        return (data["creator"]["username"])


def query_sequenceid(img_id):
    response = requests.get(
    const.MAPILLARY_GRAPH_URL.format(img_id),
    params={"access_token": access_tokens[current_token],
            "fields" : "sequence"},
    )
    data = response.json()
    return data["sequence"]

def query_sequence(sequence_id):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format("image_ids"),
        params={"access_token": access_tokens[current_token],
                "sequence_id": sequence_id},
    )

    # if rate limit is reached, try with other access token
    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)

    data = response.json()["data"]
    return [x['id'] for x in data]


def query_cangle(img_id):
    response = requests.get(
    const.MAPILLARY_GRAPH_URL.format(img_id),
    params={"access_token": access_tokens[current_token],
            "fields" : "computed_compass_angle"},
    )
    data = response.json()
    return data["computed_compass_angle"]

# compute correct yaw given the the front of the camera
# TODO % (2 *np.pi) instead?
def compute_yaw(camera_angle, direction_of_travel):
    yaw = (camera_angle-direction_of_travel) / 180 * np.pi
    # adjust to 2pi
    if yaw < 0:
        return 2 * np.pi + yaw
    elif yaw > 2 * np.pi:
        return yaw - 2 * np.pi
    else:
        return yaw

    

def compute_direction_of_travel(img_id, mapillary_token, k_neighbors=2):
    sequence_id = query_sequenceid(img_id) # TODO: provide with initial query?
    # get images before and after
    sequence = query_sequence(sequence_id) # https://graph.mapillary.com/image_ids?access_token=MLY|5381465351976670|a4ac3be1ecdebf1885b0790b8efec369&sequence_id=IfrlQqGaVsbKZm8ShcBO0u
    img_pos = sequence.index(img_id)
    sequence_neighbors = sequence[max(0, img_pos-k_neighbors):min(len(sequence), img_pos+k_neighbors+1)]

    # get coords of all neighbors
    points = []
    for img in sequence_neighbors:
        points.append(query_coords(img), mapillary_token)
    return compute_compass_angle(points)


# compute compass angle of fitted line
def compute_compass_angle(points):
    x = [point[0] for point in points]
    y = [point[1] for point in points]

    # transform crs to projected such that angles are coorect
    gdf = gpd.GeoDataFrame({"seq": list(range(0, len(x)))}, geometry=gpd.points_from_xy(x, y), crs='EPSG:4326')
    coords = gdf.to_crs(3035).get_coordinates()

    # Fit a linear regression line
    slope, _ = np.polyfit(coords["x"], coords["y"], 1)
    # Compute the angle of the line in radians
    angle = np.arctan(slope)
    # Convert the angle to angle in degrees 
    degrees = np.degrees(angle)
    # Convert to compass angle
    compass_angle = 90 - degrees

    # mind the direction
    # if right to left, then + 180
    if (x[0] > x[-1]):
        compass_angle += 180
    return compass_angle

   

def pano_to_persp(in_path, out_path, img_id, mapillary_token, 
                  cangle=None, direction_of_travel=None, persp_height = 480, persp_width = 640):
    """ Transform panorama image to two perspective images that face the direction of travel
    Args: in_path (str): path to panorama image
            out_path (str): path to save perspective images
            img_id (str): image id
            cangle (float): computed camera angle (as given by mapillary). If None, cangle is queried from mapillary
            persp_height (int): height of perspective image
            persp_width (int): width of perspective image
    """    
    os.makedirs(out_path, exist_ok=True)

    if cangle is None:
        cangle = query_cangle(img_id)

    if direction_of_travel is None:
        direction_of_travel = compute_direction_of_travel(img_id, mapillary_token)
    yaw = compute_yaw(cangle, direction_of_travel)

    # rotations
    for i in (0, 1): # front and back. If sides should also be included: 1/2, 3/2
        rots = {
            'roll': 0.,
            'pitch': 0,  # rotate vertical (look up and down)
            'yaw': yaw + (i * np.pi),  # rotate horizontal (look left and right) - np.pi = 180 degrees
        }

        equi_img = Image.open(os.path.join(in_path, f"{img_id}.jpg"))
        equi_img = np.asarray(equi_img)
        equi_img = np.transpose(equi_img, (2, 0, 1))
        
        # Run equi2pers
        pers_img = equilib.equi2pers(
            equi=equi_img,
            rots=rots,
            height=persp_height, # height, width (int): perspective size
            width=persp_width, # height, width (int): perspective size
            fov_x=90.0, # perspective image fov of x-axis
            mode="bilinear",
        )

        # transpose back to image format
        pers_img = np.transpose(pers_img, (1, 2, 0))

        Image.fromarray(pers_img).save(os.path.join(out_path, f"{img_id}_{i}.jpg"))


def format_predictions(model_prediction, pano):
    """Bring model prediction output into a format for further analysis

    Args:
        model_prediction (pd.DataFrame): model prediction csv output
        pano (bool, optional): are images panorama images with `Image`indication direction (_0 and _1)?. Defaults to True.

    Returns:
        pd.DataFrame: formatted model predictions
    """

    type_prediction = model_prediction.loc[model_prediction["Level"] == "type"].copy()
    # the prediction holds a value for each surface and a class probability. Only keep the highest prob.
    idx = type_prediction.groupby("Image")["Prediction"].idxmax()
    type_prediction = type_prediction.loc[idx]   
    type_prediction.rename(columns={"Prediction": "type_class_prob", "Level_0": "type_pred"}, inplace=True)

    quality_prediction = model_prediction.loc[model_prediction["Level"] == "quality"].copy()
    quality_prediction.rename(columns={"Prediction": "quality_pred", "Level_1": "quality_pred_label"}, inplace=True)

    pred = type_prediction.set_index("Image").join(quality_prediction.set_index("Image"), 
                                                   lsuffix="_type", rsuffix="_quality")
    pred = pred[["type_pred", "type_class_prob", "quality_pred", "quality_pred_label"]]
    pred.reset_index(inplace=True)

    if pano:
        img_ids = pred["Image"].str.split("_").str[0:2]
        pred.insert(0, "img_id", [img_id[0] for img_id in img_ids])
        pred.insert(1, "direction", [int(float(img_id[1])) for img_id in img_ids])
        pred.drop(columns=["Image"], inplace=True)
    if not pano:
        pred.rename(columns={"Image": "img_id"}, inplace=True)
    return (pred)

def format_scenery_predictions(model_prediction, pano):
    """Bring model scenery prediction output into a format for further analysis

    Args:
        model_prediction (pd.DataFrame): model prediction csv output
        pano (bool, optional): are images panorama images with `Image`indication direction (_0 and _1)?. Defaults to True.

    Returns:
        pd.DataFrame: formatted model predictions
    """

    # the prediction holds a value for each surface and a class probability. Only keep the highest prob.
    idx = model_prediction.groupby("Image")["Prediction"].idxmax()
    model_prediction = model_prediction.loc[idx]   
    model_prediction.rename(columns={"Prediction": "type_class_prob", "Level_0": "scenery_pred"}, inplace=True)

    model_prediction = model_prediction[["Image", "scenery_pred"]]

    if pano:
        img_ids = model_prediction["Image"].str.split("_").str[0:2]
        model_prediction.insert(0, "img_id", [img_id[0] for img_id in img_ids])
        model_prediction.insert(1, "direction", [int(float(img_id[1])) for img_id in img_ids])
        model_prediction.drop(columns=["Image"], inplace=True)
    if not pano:
        model_prediction.rename(columns={"Image": "img_id"}, inplace=True)
    return (model_prediction)


def execute_sql_query(dbname, query, params, is_file=True):

    conn = psycopg2.connect(
        dbname=dbname,
        user=db.user,
        host=db.host,
    )
    # create table with sample data points
    if is_file:
        with open(query, "r") as file:
            query = file.read()
    with conn.cursor(cursor_factory=DictCursor) as cursor:
        cursor.execute(sql.SQL(query.format(**params)))
        conn.commit()

    conn.close()