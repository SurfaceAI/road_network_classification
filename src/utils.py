import os
import csv
import mercantile
import requests
from vt2geojson.tools import vt_bytes_to_geojson
import time

import config

# set access tokens
with open(config.token_path, "r") as file:
    # access_token = tokenfile.readlines()
    access_tokens = [line.strip() for line in file.readlines()]
current_token = 0


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


def get_tile_images(tile):
    """Get information about images (img_id, creator_id, captured_at, is_pano, organization_id) contained within given tile (based on tiles endpoint)
    This does not include coordinates of images!

    Args:
        tile(mercantile.Tile): mercantile tile

    Returns:
        dict: all images within tile as json (dict) including properties: img_id, creator_id, captured_at, is_pano, organization_id.
    """
    global current_token

    response = requests.get(
        config.mapillary_tile_url.format(
            config.tile_coverage, int(tile.z), int(tile.x), int(tile.y)
        ),
        params={"access_token": access_tokens[current_token]},
    )

    # if rate limit is reached, try with other access token
    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)
        current_token = abs(current_token - 1)  # switch between 0 and 1 and try again
        response = requests.get(
            config.mapillary_tile_url.format(
                config.tile_coverage, int(tile.z), int(tile.x), int(tile.y)
            ),
            params={"access_token": access_tokens[current_token]},
        )

    # return response
    return vt_bytes_to_geojson(
        response.content, tile.x, tile.y, tile.z, layer=config.tile_layer
    )


def get_images_metadata(tile):
    """Get metadata for all images within a tile from mapillary (based on https://graph.mapillary.com/:image_id endpoint)
    This includes coordinates of images!

    Args:
        tile(mercantile.Tile): mercantile tile

    Returns:
        tuple(list, list(list))): Metadata of all images within tile, including coordinates, as tuple: first element is list with column names ("header"). Second element is a list of list, each list representing one image.
    """
    global current_token
    header = [
        "tile_id",
        "id",
        "sequence_id",
        "captured_at",
        "compass_angle",
        "is_pano",
        "creator_id",
        "lon",
        "lat",
    ]
    output = list()
    response = requests.get(
        config.mapillary_tile_url.format(
            config.tile_coverage, int(tile.z), int(tile.x), int(tile.y)
        ),
        params={"access_token": access_tokens[current_token]},
    )
    data = vt_bytes_to_geojson(
        response.content, tile.x, tile.y, tile.z, layer=config.tile_layer
    )

    # a feature is a point/image
    # TODO: can this be speed up?
    for feature in data["features"]:
        output.append(
            [
                str(int(tile.x)) + "_" + str(int(tile.y)) + "_" + str(int(tile.z)),
                feature["properties"]["id"],
                feature["properties"]["sequence_id"],
                feature["properties"]["captured_at"],
                feature["properties"]["compass_angle"],
                feature["properties"]["is_pano"],
                feature["properties"]["creator_id"],
                feature["geometry"]["coordinates"][0],
                feature["geometry"]["coordinates"][1],
            ]
        )

    return (header, output)


def download_image(image_id, image_folder):
    """Download image file based on image_id and save to given image_folder

    Args:
        image_id (str): ID of image to download
        image_folder (str): path of folder to save image to
    """
    response = requests.get(
        config.mapillary_graph_url.format(image_id),
        params={
            "fields": config.image_size,
            "access_token": access_tokens[current_token],
        },
    )

    if response.status_code != 200:
        print(response.status_code)
        print(response.reason)
        print(f"image_id: {image_id}")
    else:
        data = response.json()
        if config.image_size in data:
            image_url = data[config.image_size]

            # image: save each image with ID as filename to directory by sequence ID
            image_name = "{}.jpg".format(image_id)
            image_path = os.path.join(image_folder, image_name)
            with open(image_path, "wb") as handler:
                image_data = requests.get(image_url, stream=True).content
                handler.write(image_data)
        else:
            print(f"no image size {config.image_size} for image {image_id}")


def query_and_write_img_metadata(tiles, out_path, minLon, minLat, maxLon, maxLat):
    """Write metadata of all images in tiles to csv

    Args:
        tiles (df): dataframe with tiles and columns x,y,z,lat,lon
        out_path (str): path to save csv with image metadata of tile to
    """
    # write metadata of all potential images to csv
    with open(out_path, "w", newline="") as csvfile:
        csvwriter = csv.writer(csvfile)
        for i in range(0, len(tiles)):
            if i % 10 == 0:
                print(f"{i} tiles of {len(tiles)}")
            tile = tiles.iloc[
                i,
            ]
            header, output = get_images_metadata(tile)
            if i == 0:
                csvwriter.writerow(header)
            for row in output:
                # filter img
                if ((row[header.index("lon")] > minLon) and (row[header.index("lon")] < maxLon) 
                    and (row[header.index("lat")] > minLat) and (row[header.index("lat")] < maxLat)
                    and row[header.index("is_pano")] == False):
                    
                    csvwriter.writerow(row)


def download_images(metadata_path, img_folder):
    start = time.time()
    os.makedirs(img_folder, exist_ok=True)

    with open(
        metadata_path, newline=""
    ) as csvfile:
        csvreader = csv.reader(csvfile)
        image_ids = [row[1] for row in csvreader][1:]
        for i in range(0, len(image_ids)):
            if i % 100 == 0:
                print(f"{i} images downloaded")
            download_image(
                int(image_ids[i]), img_folder
            )
    print(f"{round((time.time()-start )/ 60)} mins")


def write_tiles_within_boundary(csv_path, boundary=None, minLon=None, minLat=None, maxLon=None, maxLat=None):
        
    if boundary:
        bbox = boundary.total_bounds
    else:
        bbox = (minLon, minLat, maxLon, maxLat)

    tiles = list()
    tiles += list(
        mercantile.tiles(bbox[0], bbox[1], bbox[2], bbox[3], config.zoom)
    )

    # with open(
    #     csv_path, "w", newline=""
    # ) as csvfile:
    #     csvwriter = csv.writer(csvfile)
    #     csvwriter.writerow(["x", "y", "z", "lat", "lon"])
    #     for i in range(0, len(tiles)):
    #         tile = tiles[i]
    #         lon, lat = tile_center(tile.x, tile.y, config.zoom)
    #         point = gpd.GeoDataFrame(
    #             geometry=[Point(lon, lat)], crs="EPSG:4326"
    #         )
    #         # if tile center within boundary of city, write to csv
    #         if boundary.geometry.contains(point)[0]:
    #             csvwriter.writerow([tile.x, tile.y, config.zoom, lat, lon])

    return tiles