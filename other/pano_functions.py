import os
import sys
from pathlib import Path

import equilib
import geopandas as gpd
import numpy as np
import requests
from PIL import Image

# import matplotlib.pyplot as plt

root_path = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_path))

import constants as const


def query_coords(img_id, mapillary_token):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format(img_id),
        params={"access_token": mapillary_token, "fields": "geometry"},
    )
    data = response.json()
    return data["geometry"]["coordinates"]

    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)
        print(f"image_id: {image_id}")
        return False
    else:
        data = response.json()
        return data["creator"]["username"]


def query_sequenceid(img_id):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format(img_id),
        params={"access_token": access_tokens[current_token], "fields": "sequence"},
    )
    data = response.json()
    return data["sequence"]


def query_sequence(sequence_id):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format("image_ids"),
        params={
            "access_token": access_tokens[current_token],
            "sequence_id": sequence_id,
        },
    )

    # if rate limit is reached, try with other access token
    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)

    data = response.json()["data"]
    return [x["id"] for x in data]


def query_cangle(img_id):
    response = requests.get(
        const.MAPILLARY_GRAPH_URL.format(img_id),
        params={
            "access_token": access_tokens[current_token],
            "fields": "computed_compass_angle",
        },
    )
    data = response.json()
    return data["computed_compass_angle"]


# compute correct yaw given the the front of the camera
# TODO % (2 *np.pi) instead?
def compute_yaw(camera_angle, direction_of_travel):
    yaw = (camera_angle - direction_of_travel) / 180 * np.pi
    # adjust to 2pi
    if yaw < 0:
        return 2 * np.pi + yaw
    elif yaw > 2 * np.pi:
        return yaw - 2 * np.pi
    else:
        return yaw


def compute_direction_of_travel(img_id, mapillary_token, k_neighbors=2):
    sequence_id = query_sequenceid(img_id)  # TODO: provide with initial query?
    # get images before and after
    sequence = query_sequence(
        sequence_id
    )  # https://graph.mapillary.com/image_ids?access_token=MLY|5381465351976670|a4ac3be1ecdebf1885b0790b8efec369&sequence_id=IfrlQqGaVsbKZm8ShcBO0u
    img_pos = sequence.index(img_id)
    sequence_neighbors = sequence[
        max(0, img_pos - k_neighbors) : min(len(sequence), img_pos + k_neighbors + 1)
    ]

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
    gdf = gpd.GeoDataFrame(
        {"seq": list(range(0, len(x)))},
        geometry=gpd.points_from_xy(x, y),
        crs="EPSG:4326",
    )
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
    if x[0] > x[-1]:
        compass_angle += 180
    return compass_angle


def pano_to_persp(
    in_path,
    out_path,
    img_id,
    mapillary_token,
    cangle=None,
    direction_of_travel=None,
    persp_height=480,
    persp_width=640,
):
    """Transform panorama image to two perspective images that face the direction of travel
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
    for i in (0, 1):  # front and back. If sides should also be included: 1/2, 3/2
        rots = {
            "roll": 0.0,
            "pitch": 0,  # rotate vertical (look up and down)
            "yaw": yaw
            + (
                i * np.pi
            ),  # rotate horizontal (look left and right) - np.pi = 180 degrees
        }

        equi_img = Image.open(os.path.join(in_path, f"{img_id}.jpg"))
        equi_img = np.asarray(equi_img)
        equi_img = np.transpose(equi_img, (2, 0, 1))

        # Run equi2pers
        pers_img = equilib.equi2pers(
            equi=equi_img,
            rots=rots,
            height=persp_height,  # height, width (int): perspective size
            width=persp_width,  # height, width (int): perspective size
            fov_x=90.0,  # perspective image fov of x-axis
            mode="bilinear",
        )

        # transpose back to image format
        pers_img = np.transpose(pers_img, (1, 2, 0))

        Image.fromarray(pers_img).save(os.path.join(out_path, f"{img_id}_{i}.jpg"))
