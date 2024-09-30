import os
import logging
import utils
import constants as const

import csv
import mercantile
from tqdm import tqdm
import requests
from vt2geojson.tools import vt_bytes_to_geojson
import time

from itertools import repeat
from tqdm import tqdm
import concurrent.futures


class MapillaryInterface:
    def __init__(self, mapillary_token, parallel, parallel_batch_size):
        self.token = mapillary_token
        self.parallel = parallel
        self.parallel_batch_size = parallel_batch_size

    
    def tiles_within_boundary(self, boundary=None, minLon=None, minLat=None, maxLon=None, maxLat=None):
        if boundary:
            bbox = boundary.total_bounds
        else:
            bbox = (minLon, minLat, maxLon, maxLat)

        tiles = list()
        tiles += list(
            mercantile.tiles(bbox[0], bbox[1], bbox[2], bbox[3], const.ZOOM)
        )
        return tiles

    def query_metadata(self, aoi):

        logging.info(f"Querying img metadata in bbox {[aoi.minLon, aoi.minLat, aoi.maxLon, aoi.maxLat]}")
        # get all relevant tile ids
        tiles = self.tiles_within_boundary(minLon=aoi.minLon, 
                                           minLat=aoi.minLat, 
                                           maxLon=aoi.maxLon, 
                                           maxLat=aoi.maxLat)
       
        out_path = aoi.set_img_metadata_path()
        # download img metadata to csv: to that if download is interrupted, we can resume
        # TODO: write directly into DB after each tile (without csv?)
        with open(out_path, "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile)
            for i in tqdm(range(0, len(tiles))):
                tile = tiles[i]
                header, output = self.metadata_in_tile(tile)
                if i == 0:
                    csvwriter.writerow(header)
                for row in output:
                    # filter img
                    if ((row[header.index("lon")] > aoi.minLon) and (row[header.index("lon")] < aoi.maxLon) 
                        and (row[header.index("lat")] > aoi.minLat) and (row[header.index("lat")] < aoi.maxLat)):

                        if aoi.no_pano and row[header.index("is_pano")] == True:
                            continue
                        if aoi.userid and str(row[header.index("creator_id")]) != aoi.userid:
                            continue
                        
                        csvwriter.writerow(row)

        logging.info(f"img metadata query complete")

    def metadata_in_tile(self, tile):
        """Get metadata for all images within a tile from mapillary (based on https://graph.mapillary.com/:image_id endpoint)
        This includes coordinates of images!

        Args:
            tile(mercantile.Tile): mercantile tile

        Returns:
            tuple(list, list(list))): Metadata of all images within tile, including coordinates, as tuple: first element is list with column names ("header"). Second element is a list of list, each list representing one image.
        """
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
            const.MAPILLARY_TILE_URL.format(
                const.TILE_COVERAGE, int(tile.z), int(tile.x), int(tile.y)
            ),
            params={"access_token": self.token},
        )
        data = vt_bytes_to_geojson(
            response.content, tile.x, tile.y, tile.z, layer=const.TILE_LAYER
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
    


    def download_images(self, image_ids, img_size, dest_folder):
        start = time.time()
        os.makedirs(dest_folder, exist_ok=True)

        if self.parallel:
            # only download batch_size at a time (otherwise we get connectionErrors with Mapillary)
            if (self.parallel_batch_size is None) or (self.parallel_batch_size > len(image_ids)):
                parallel_batch_size = len(image_ids)
            else:
                parallel_batch_size = self.parallel_batch_size
            for batch_start in tqdm(range(0, len(image_ids), parallel_batch_size)):

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    batch_end = (batch_start+parallel_batch_size 
                                if batch_start+parallel_batch_size < len(image_ids) 
                                else len(image_ids))
                    
                    executor.map(self.download_image, image_ids[batch_start:batch_end], 
                                repeat(img_size), repeat(dest_folder))
        else:
            for i in tqdm(range(0, len(image_ids))):
                self.download_image(
                    int(image_ids[i]), img_size, dest_folder
                )
        logging.INFO(f"{round((time.time()-start )/ 60)} mins")



    def download_image(self, img_id, img_size, dest_folder):
        """Download image file based on img_id and save to given image_folder

        Args:
            img_id (str): ID of image to download
            img_size (str): size of image to download (e.g. thumb_1024_url, thumb_2048_url, thumb_original_url)
            img_folder (str): path of folder to save image to
        """
        response = requests.get(
            const.MAPILLARY_GRAPH_URL.format(img_id),
            params={
                "fields": img_size,
                "access_token": self.token,
            },
        )

        if response.status_code != 200:
            logging.INFO(response.status_code)
            logging.INFO(response.reason)
            logging.INFO(f"image_id: {img_id}")
        else:
            data = response.json()
            if img_size in data:
                image_url = data[img_size]

                # image: save each image with ID as filename
                image_data = requests.get(image_url, stream=True).content
                with open(os.path.join(dest_folder, f"{img_id}.jpg"), "wb") as handler:
                    handler.write(image_data)
            else:
                logging.INFO(f"no image size {img_size} for image {img_id}")


