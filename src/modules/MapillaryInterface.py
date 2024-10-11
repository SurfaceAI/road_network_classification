import concurrent.futures
import logging
import os
import sys
import io
import time
from itertools import repeat
from pathlib import Path
from PIL import Image

import requests
from requests.exceptions import ConnectTimeout
from tqdm import tqdm
from vt2geojson.tools import vt_bytes_to_geojson

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
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


    def query_mapillary(self, request_url, request_params, request_timeout=10, max_retries=10):
        max_retries = 10
        retries = 0
        while retries < max_retries:
            try:
                response = requests.get(
                    request_url,
                    params=request_params,
                    timeout=request_timeout,
                )
                if response.status_code != 200:
                    logging.info(response.status_code)
                    logging.info(response.reason)
                    #logging.info(f"image_id: {img_id}")
                    return None
                else:
                    return response
            except ConnectTimeout:
                retries += 1
                wait_time =  (2 ** (retries - 1))*60
                logging.info(f"Connection timed out. Retrying in {wait_time/60} minutes...")
                time.sleep(wait_time)
            except requests.exceptions.RequestException as e:
                print(f"Request failed: {e}")
                return None
        return None
    
   
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
        response = self.query_mapillary(
            const.MAPILLARY_TILE_URL.format(
                const.TILE_COVERAGE, int(tile.z), int(tile.x), int(tile.y)
            ),
            {"access_token": self.token}
        )
        if response is None:
            return (header, None)
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


    def query_img (self, img_id, img_size):
        response = self.query_mapillary(
            const.MAPILLARY_GRAPH_URL.format(int(img_id)),
            {
                "fields": img_size,
                "access_token": self.token,
            }
        )
        if response is not None:
            data = response.json()
            if img_size in data:
                response = self.query_mapillary(data[img_size], {})
                if response is not None:
                    return Image.open(io.BytesIO(response.content))
            else:
                logging.info(f"no image size {img_size} for image {img_id}")
        return None

    def query_imgs(self, img_ids, img_size):
        """Query img content urls for given Mapillary img ids

        Args:
            img_ids (list): img ids to query urls for
            img_size (str): Size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)

        Returns:
            list: img_urls
        """
        imgs = []

        if self.parallel:
            # only download batch_size at a time (otherwise we get connectionErrors with Mapillary)
            if (self.parallel_batch_size is None) or (
                self.parallel_batch_size > len(img_ids)
            ):
                parallel_batch_size = len(img_ids)
            else:
                parallel_batch_size = self.parallel_batch_size

            for i in range(0, len(img_ids), parallel_batch_size):
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    j = min(
                        i + parallel_batch_size,
                        len(img_ids))

                    batch_imgs = list(executor.map(
                        self.query_img,
                        img_ids[i:j],
                        repeat(img_size)
                    ))
                    imgs.extend(batch_imgs)

        else:
            for i in tqdm(range(0, len(img_ids))):
                img_id = img_ids[i]
                img = self.query_img(img_id, img_size)
                imgs.append(img)
        
        imgs = [item for item in imgs if item is not None]
        return imgs

