import os
import sys
from pathlib import Path

import mercantile
import numpy as np
import logging

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
from tqdm import tqdm

import constants as const


class AreaOfInterest:
    """The area of interest, defined by a bounding box, that the surface is classified for."""

    def __init__(self, config: dict):
        """
        Initializes an AreaOfInterest instance.

        Args:
            config (dict): Configuration dictionary containing the following keys:
                - name (str): The name of the area of interest (aoi).
                - run (str): The run identifier, if different variations are tested.
                - minLon (float): The minimum longitude of the bounding box (in EPSG:4326).
                - minLat (float): The minimum latitude of the bounding box (in EPSG:4326).
                - maxLon (float): The maximum longitude of the bounding box (in EPSG:4326).
                - maxLat (float): The maximum latitude of the bounding box (in EPSG:4326).
                - proj_crs (int): Which projected CRS to use for computations.
                - img_size (str): The size of the image for Mapillary download.
                - dist_from_road (int): The distance from the road to consider for image selection.
                - min_road_length (int, optional): Minimum road length.
                - segment_length (int, optional): Length of subsegments for aggregation algorithm.
                - segments_per_group (int, optional): Number of segments per group.
                - additional_id_column (str, optional): Additional column to use as an ID for custom road networks. Defaults to None.
        """

        # TODO: verify config inputs
        self.config = config

        self.name = config.get("name")
        self.run = config.get("run", None)
        self.minLon = config.get("minLon")
        self.minLat = config.get("minLat")
        self.maxLon = config.get("maxLon")
        self.maxLat = config.get("maxLat")
        self.proj_crs = config.get("proj_crs")

        # img variables
        self.img_size = config.get("img_size", "thumb_1024_url")
        self.userid =  config.get("userid", False) # only limited to a specific user id? TODO: implement
        self.use_pano =  config.get("use_pano", False) # exclude panoramic images # TODO: implement inclusion
        self.dist_from_road = config.get("dist_from_road")

        # road network variables
        self.min_road_length = config.get("min_road_length")
        self.segment_length = config.get("segment_length")
        self.segments_per_group = config.get("segments_per_group", None)

        # customizations
        self.additional_id_column = config.get("additional_id_column", None)
        self.query_params = self._get_query_params()

    def _get_query_params(self):
        additional_id_column = (
            f"{self.additional_id_column},"
            if self.additional_id_column is not None
            else ""
        )  # add comma
        grouping_ids = f"{additional_id_column}id,part_id,group_num"
        additional_ways_id_column = (
            f"ways.{additional_id_column}" if additional_id_column != "" else ""
        )
        # if segments_per_group is None, then the eval groups are the entire length of the road segment,
        # # i.e., all subsegments have group_num 1
        group_num = (
            "0"
            if self.segments_per_group == None
            else f"segment_number / {self.segments_per_group}"
        )

        return {
            "name": self.name,
            "bbox0": self.minLon,
            "bbox1": self.minLat,
            "bbox2": self.maxLon,
            "bbox3": self.maxLat,
            "crs": self.proj_crs,
            "dist_from_road": self.dist_from_road,
            "additional_id_column": additional_id_column,
            "additional_ways_id_column": additional_ways_id_column,
            "grouping_ids": grouping_ids,
            "segment_length": self.segment_length,
            "group_num": group_num,
            "segments_per_group": self.segments_per_group,
            "min_road_length": self.min_road_length,
        }


    def get_and_write_img_metadata(self, mi, db):
        # get all relevant tile ids
        db.execute_sql_query(const.SQL_CREATE_IMG_METADATA_TABLE, self.query_params)

        tiles = list(
            mercantile.tiles(
                self.minLon, self.minLat, self.maxLon, self.maxLat, const.ZOOM
            )
        )

        for i in tqdm(range(0, len(tiles))):
            # TODO: parallellize?
            tile = tiles[i]
            header, output = mi.metadata_in_tile(tile)
            rows = np.array(output)
            if len(rows) == 0:
                continue
            rows = rows[
                (rows[:, header.index("lon")].astype(float) >= self.minLon)
                & (rows[:, header.index("lon")].astype(float) <= self.maxLon)
                & (rows[:, header.index("lat")].astype(float) >= self.minLat)
                & (rows[:, header.index("lat")].astype(float) <= self.maxLat)
            ]
            if not self.use_pano and len(rows) > 0:
                rows = rows[rows[:, header.index("is_pano")] == "False"]
            if self.userid and len(rows) > 0:
                rows = rows[rows[:, header.index("creator_id")] == self.userid]
            if len(rows) > 0:
                db.add_rows_to_table(f"{self.name}_img_metadata", header, rows)
        db.execute_sql_query(const.SQL_ADD_GEOM_COLUMN, self.query_params)

    def classify_images(self, mi, db, md):
        img_ids = db.img_ids_from_dbtable(f"{self.name}_img_metadata")
        if (db.table_exists(f"{self.name}_img_classifications")):
            existing_img_ids = db.img_ids_from_dbtable(f"{self.name}_img_classifications")
            logging.info(f"existing classified images: {len(existing_img_ids)}")
            img_ids = list(set(img_ids) - set(existing_img_ids))
            logging.info(f"remaining new images to classify: {len(img_ids)}")

        db.execute_sql_query(const.SQL_PREP_MODEL_RESULT, self.query_params)

        for i in tqdm(
            range(0, len(img_ids), md.batch_size), desc="Download and classify images"
        ):
            j = min(i + md.batch_size, len(img_ids))

            img_data = mi.query_imgs(
                img_ids[i:j],
                self.img_size,
            )
            model_output = md.batch_classifications(img_data)

            # add img_id to model_output
            # start = time.time()
            value_list = [
                [img_id] + mo for img_id, mo in zip(img_ids[i:j], model_output)
            ]
            header = [
                "img_id",
                "road_type_pred",
                "road_type_prob",
                "type_pred",
                "type_class_prob",
                "quality_pred",
            ]
            db.add_rows_to_table(f"{self.name}_img_classifications", header, value_list)
            # print(f"db insert {time.time() - start}")

        db.execute_sql_query(const.SQL_RENAME_ROAD_TYPE_PRED, self.query_params)
