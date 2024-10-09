import os
from pathlib import Path
import sys
import logging

import csv
import mercantile
from tqdm import tqdm
import requests
from requests.exceptions import ConnectTimeout
from vt2geojson.tools import vt_bytes_to_geojson
import time

from itertools import repeat
from tqdm import tqdm
import concurrent.futures


# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
import utils
import constants as const


class MapillaryInterface:
    """Interface for Mapillary API to query image metadata and download images."""

    def __init__(self, mapillary_token, parallel, parallel_batch_size):
        """Initializes a MapillaryInterface object.
                    Zoom level is defined in constants.py.


        Args:
            mapillary_token (str): Mapillary API token
            parallel (bool): Download images in parallel
            parallel_batch_size (int): Number of images to download in parallel
        """
        self.token = mapillary_token
        self.parallel = parallel
        self.parallel_batch_size = parallel_batch_size

    def tiles_within_boundary(self, boundary, zoom):
        """
        Calculates and returns the tiles that fall within a given boundary defined by a polygon (GeoDataFrame).

        Args:
            boundary (GeoDataFrame): A GeoDataFrame representing the boundary.

        Returns:
            list: A list of mercantile.Tiles that are within the bbox.
        """
        bbox = boundary.total_bounds
        return self.tiles_within_bbox(bbox, zoom)

    def tiles_within_bbox(self, bbox, zoom):
        """
        Calculates and returns the tiles that fall within a bounding box.

        Args:
            bbox (list): A list of bounding box coordinates [minLon, minLat, maxLon, maxLat]
            zoom (int): Zoom level of the mercantile tiles

        Returns:
            list: A list of mercantile.Tiles that are within the bbox.
        """
        return list(mercantile.tiles(bbox[0], bbox[1], bbox[2], bbox[3], zoom))

   
    def query_metadata(self, aoi):
        """
        Query image metadata within a given area of interest and save to csv file.
        """
        # get all relevant tile ids
        tiles = self.tiles_within_bbox(
            [aoi.minLon, aoi.minLat, aoi.maxLon, aoi.maxLat], const.ZOOM
        )

        # download img metadata to csv: so that if download is interrupted, we can resume
        # TODO: write directly into DB after each tile (without csv?)
        with open(aoi.img_metadata_path, "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile)
            for i in tqdm(range(0, len(tiles))):
                tile = tiles[i]
                header, output = self.metadata_in_tile(tile)

                if i == 0:
                    csvwriter.writerow(header)
                if output:
                    for row in output:
                        # filter img in bbox
                        if (
                            (row[header.index("lon")] > aoi.minLon)
                            and (row[header.index("lon")] < aoi.maxLon)
                            and (row[header.index("lat")] > aoi.minLat)
                            and (row[header.index("lat")] < aoi.maxLat)
                        ):
                            if (
                                not aoi.use_pano
                                and row[header.index("is_pano")] == True
                            ):
                                continue
                            if (
                                aoi.userid
                                and str(row[header.index("creator_id")]) != aoi.userid
                            ):
                                continue

                            csvwriter.writerow(row)

        logging.info(f"img metadata query complete")

    def metadata_in_tile(self, tile):
        """Get metadata for all images within a tile from mapillary (based on https://graph.mapillary.com/:image_id endpoint)
        Args:
            tile(mercantile.Tile): mercantile tile

        Returns:
            tuple(list, list(list))): Metadata of all images within tile, including coordinates, as tuple: first element is list with column names ("header"). Second element is a list of list, each list representing one image.
        """
        header = [
            "img_id",
            "tile_id",
            "sequence_id",
            "captured_at",
            "compass_angle",
            "is_pano",
            "creator_id",
            "lon",
            "lat",
        ]
        tile_id = str(int(tile.x)) + "_" + str(int(tile.y)) + "_" + str(int(tile.z))

        output = list()
        response = requests.get(
            const.MAPILLARY_TILE_URL.format(
                const.TILE_COVERAGE, int(tile.z), int(tile.x), int(tile.y)
            ),
            params={"access_token": self.token},
        )

        if response.status_code != 200:
            logging.info(response.status_code)
            logging.info(response.reason)
            logging.info(f"tile_id: {tile_id}")

            return (header, False)

        else:
            data = vt_bytes_to_geojson(
                response.content, tile.x, tile.y, tile.z, layer=const.TILE_LAYER
            )

            # a feature is a point/image
            # TODO: can this be speed up?
            for feature in data["features"]:
                output.append(
                    [
                        feature["properties"]["id"],
                        tile_id,
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


    def query_img_url(self, img_id, img_size):
        max_retries = 10
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(
                    const.MAPILLARY_GRAPH_URL.format(int(img_id)),
                    params={
                        "fields": img_size,
                        "access_token": self.token,
                    },
                    timeout=10,
                )
                if response.status_code != 200:
                    logging.info(response.status_code)
                    logging.info(response.reason)
                    logging.info(f"image_id: {img_id}")
                else:
                    data = response.json()
                    if img_size in data:
                        return data[img_size]
                    else:
                        logging.info(f"no image size {img_size} for image {img_id}")
            except ConnectTimeout:
                retries += 1
                wait_time =  (2 ** (retries - 1))*60
                logging.info(f"Connection timed out. Retrying in {wait_time/60} minutes...")
                time.sleep(wait_time)
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return None

        return None

    def query_img_urls(self, img_ids, img_size):
        """Query img content urls for given Mapillary img ids

        Args:
            img_ids (list): img ids to query urls for
            img_size (str): Size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)

        Returns:
            list: img_urls
        """
        img_urls = []

        if self.parallel:
            # only download batch_size at a time (otherwise we get connectionErrors with Mapillary)
            if (self.parallel_batch_size is None) or (
                self.parallel_batch_size > len(img_ids)
            ):
                parallel_batch_size = len(img_ids)
            else:
                parallel_batch_size = self.parallel_batch_size
            for batch_start in tqdm(range(0, len(img_ids), parallel_batch_size)):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    batch_end = (
                        batch_start + parallel_batch_size
                        if batch_start + parallel_batch_size < len(img_ids)
                        else len(img_ids)
                    )

                    batch_img_urls = list(executor.map(
                        self.query_img_url,
                        img_ids[batch_start:batch_end],
                        repeat(img_size)
                    ))
                    img_urls.extend(batch_img_urls)

        else:
            for i in tqdm(range(0, len(img_ids))):
                img_id = img_ids[i]
                img_url = self.query_img_url(img_id, img_size)
                img_urls.append(img_url)
        img_urls = [item for item in img_urls if item is not None]
        return img_urls


#######
# def download_imgs_from_table(
#     self,
#     dest_folder,
#     img_size,
#     csv_path=None,
#     img_id_col=1,
#     database=None,
#     db_table=None,
# ):
#     """Download images based on image IDs in a csv file or database table

#     Args:
#         dest_folder (str): Destination folder to save images to
#         img_size (str): Size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)
#         csv_path (str, optional): Path to csv file with image IDs. If database table is given, set to None. Defaults to None.
#         img_id_col (int, optional): Column in csv file with image IDs. Defaults to 1.
#         database (SurfaceDatabase, optional): SurfaceDatabase object. If csv_path is given, set to None. Defaults to None.
#         db_table (str, optional): Database table with image IDs. If csv_path is given, set to None. Defaults to None.
#     """

#     if csv_path:
#         img_ids = utils.img_ids_from_csv(csv_path, img_id_col=img_id_col)
#     elif db_table:
#         img_ids = database.img_ids_from_dbtable(db_table)

#     # only download images that are not present yet in download folder
#     if os.path.exists(dest_folder):
#         imgs_in_download_folder = os.listdir(dest_folder)
#         imgIDs_in_download_folder = [
#             img_id.split(".")[0] for img_id in imgs_in_download_folder
#         ]
#         img_ids = list(set(img_ids) - set(imgIDs_in_download_folder))

#     logging.info(f"Downloading {len(img_ids)} images")
#     self.download_images(img_ids, img_size, dest_folder)

# def download_images(self, image_ids, img_size, dest_folder):
#     """Download images of given image_ids and save to given image_folder

#     Args:
#         img_ids (list): IDs of images to download
#         img_size (str): size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)
#         img_folder (str): path of folder to save image to
#     """

#     start = time.time()
#     os.makedirs(dest_folder, exist_ok=True)

#     if self.parallel:
#         # only download batch_size at a time (otherwise we get connectionErrors with Mapillary)
#         if (self.parallel_batch_size is None) or (
#             self.parallel_batch_size > len(image_ids)
#         ):
#             parallel_batch_size = len(image_ids)
#         else:
#             parallel_batch_size = self.parallel_batch_size
#         for batch_start in tqdm(range(0, len(image_ids), parallel_batch_size)):
#             with concurrent.futures.ThreadPoolExecutor() as executor:
#                 batch_end = (
#                     batch_start + parallel_batch_size
#                     if batch_start + parallel_batch_size < len(image_ids)
#                     else len(image_ids)
#                 )

#                 executor.map(
#                     self.download_image,
#                     image_ids[batch_start:batch_end],
#                     repeat(img_size),
#                     repeat(dest_folder),
#                 )
#     else:
#         for i in tqdm(range(0, len(image_ids))):
#             self.download_image(int(image_ids[i]), img_size, dest_folder)
#     logging.info(f"{round((time.time()-start )/ 60)} mins")

# def download_image(self, img_id, img_size, dest_folder):
#     """Download image file based on img_id and save to given image_folder

#     Args:
#         img_id (str): ID of image to download
#         img_size (str): size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)
#         img_folder (str): path of folder to save image to
#     """
#     response = requests.get(
#         const.MAPILLARY_GRAPH_URL.format(img_id),
#         params={
#             "fields": img_size,
#             "access_token": self.token,
#         },
#     )

#     if response.status_code != 200:
#         logging.info(response.status_code)
#         logging.info(response.reason)
#         logging.info(f"image_id: {img_id}")
#     else:
#         data = response.json()
#         if img_size in data:
#             image_url = data[img_size]

#             # image: save each image with ID as filename
#             image_data = requests.get(image_url, stream=True).content
#             with open(os.path.join(dest_folder, f"{img_id}.jpg"), "wb") as handler:
#                 handler.write(image_data)
#         else:
#             logging.info(f"no image size {img_size} for image {img_id}")
