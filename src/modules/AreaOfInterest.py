import os
import sys
from pathlib import Path

import mercantile
import numpy as np
import pandas as pd
import mercantile
from PIL import Image
import io

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
from tqdm import tqdm

import constants as const
import utils
from modules import Models


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
                - custom_sql_way_selection (bool, optional): Custom SQL query for way selection. Defaults to False.
                - custom_road_type_join (bool, optional): Custom SQL query for road type join. Defaults to False.
                - custom_attrs (dict, optional): Custom attributes for road network. Defaults to {}.
                - custom_road_type_separation (bool, optional): Custom road type separation SQL script. Defaults to False.
        """

        # TODO: verify config inputs
        self.config = config

        self.name = config.get("name")
        self.run = config.get("run")
        self.minLon = config.get("minLon")
        self.minLat = config.get("minLat")
        self.maxLon = config.get("maxLon")
        self.maxLat = config.get("maxLat")
        self.proj_crs = config.get("proj_crs")

        # img variables
        self.img_size = config.get("img_size")
        self.userid = (
            False if "userid" not in config.keys() else config.get("userid")
        )  # only limited to a specific user id? TODO: implement
        self.use_pano = (
            False if "use_pano" not in config.keys() else config.get("use_pano")
        )  # exclude panoramic images
        self.dist_from_road = config.get("dist_from_road")

        # road network variables
        self.min_road_length = config.get("min_road_length")
        self.segment_length = config.get("segment_length")
        self.segments_per_group = config.get("segments_per_group")

        # customizations
        self.additional_id_column = (
            None
            if "additional_id_column" not in config.keys()
            else config.get("additional_id_column")
        )
        self.custom_sql_way_selection = config.get("custom_sql_way_selection", False)
        self.custom_road_type_join = config.get("custom_road_type_join", False)
        self.custom_attrs = config.get("custom_attrs", {})
        self.custom_road_type_separation = config.get("custom_road_type_separation",False)

        self.query_params = self._get_query_params()
        self.custom_query_files = self._get_custom_query_files()


    def _get_query_params(self):
        additional_id_column = (
            f"{self.additional_id_column}," if self.additional_id_column is not None else ""
        )  # add comma
        grouping_ids = f"{additional_id_column}id,part_id,group_num"
        additional_ways_id_column = (
            f"ways.{additional_id_column}" if additional_id_column != "" else ""
        )
        # if segments_per_group is None, then the eval groups are the entire length of the road segment, 
        # # i.e., all subsegments have group_num 1
        group_num = '0' if self.segments_per_group == None else f"segment_number / {self.segments_per_group}"
        
        # TODO: remove
        folder = src_dir / "data" / self.run
        surface_pred_csv_path = folder / "classification_results.csv"
        road_type_pred_csv_path = folder / "scenery_class_results.csv"

        return {
            "name": self.name,
            "bbox0": self.minLon,
            "bbox1": self.minLat,
            "bbox2": self.maxLon,
            "bbox3": self.maxLat,
            "crs": self.proj_crs,
            "dist_from_road": self.dist_from_road,
            "surface_pred_csv_path": surface_pred_csv_path,
            "road_type_pred_csv_path": road_type_pred_csv_path,
            "additional_id_column": additional_id_column,
            "additional_ways_id_column": additional_ways_id_column,
            "grouping_ids": grouping_ids,
            "segment_length": self.segment_length,
            "group_num": group_num,
            "segments_per_group": self.segments_per_group,
            "min_road_length": self.min_road_length,
        }

    def _get_custom_query_files(self):
        return {
            "way_selection": (
            const.SQL_WAY_SELECTION
            if not self.custom_sql_way_selection
            else self.custom_sql_way_selection
        ),
            "roadtype_separation": (
            const.SQL_ASSIGN_ROAD_TYPES
            if not self.custom_road_type_separation
            else self.custom_road_type_separation
        )
        }

    def get_and_write_img_metadata(self, mi, db): 
        # get all relevant tile ids
        db.execute_sql_query(const.SQL_CREATE_IMG_METADATA_TABLE, self.query_params)

        tiles = list(mercantile.tiles(self.minLon, self.minLat, self.maxLon, self.maxLat,  const.ZOOM))

        for i in tqdm(range(0, len(tiles))):
            tile = tiles[i]
            header, output = mi.metadata_in_tile(tile)
            rows = np.array(output)
            if len(rows) == 0:
                continue
            rows = rows[(rows[:, header.index("lon")].astype(float) >= self.minLon) &
                       (rows[:, header.index("lon")].astype(float) <= self.maxLon) &
                       (rows[:, header.index("lat")].astype(float) >= self.minLat) &
                       (rows[:, header.index("lat")].astype(float) <= self.maxLat)]
            if not self.use_pano and len(rows) > 0:
                rows = rows[rows[:, header.index("is_pano")] == 'False']
            if self.userid and len(rows) > 0:
                rows = rows[rows[:, header.index("creator_id")] == self.userid]
            if len(rows) > 0:
                db.add_rows_to_table(f"{self.name}_img_metadata", header, rows)
        db.execute_sql_query(const.SQL_ADD_GEOM_COLUMN, self.query_params)

    def format_pred_files(self):
        file_path = self.query_params["surface_pred_csv_path"]
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        file_path = self.query_params["road_type_pred_csv_path"]
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        surface_pred = utils.format_predictions(
            pd.read_csv(self.pred_path, dtype={"Image": str, "Level_1": str}),
            is_pano=self.use_pano,
        )
        surface_pred.to_csv(self.query_params["surface_pred_csv_path"], index=False)

        road_type_pred = utils.format_scenery_predictions(
            pd.read_csv(self.road_type_pred_path, dtype={"Image": str}),
            is_pano=self.use_pano,
        )
        road_type_pred.to_csv(self.query_params["road_type_pred_csv_path"], index=False)

    def classify_images(self, mi, db, md):
        import time
        img_ids = db.img_ids_from_dbtable(f"{self.name}_img_metadata")
        #img_ids = ["1000068877331935", "1000140361462393"]

        for i in tqdm(range(0, len(img_ids), md.batch_size), desc="Download and classify images"):
            j = min(i+md.batch_size, len(img_ids))

            #start = time.time()
            img_data = mi.query_imgs(
                img_ids[i:j],
                self.img_size,
            )
            #print(f"img download {time.time() - start}")
            start = time.time()
            model_output = md.batch_classifications(img_data)
            #print(f"img classification {time.time() - start}")
            
            # TODO: current fix to turn pd to list of lists
            model_output = model_output.values.tolist()
            # add img_id to model_output
            value_list = [mo + [img_id] for img_id, mo in zip(img_ids[i:j], model_output)]
            #start = time.time()
            db.execute_many_sql_query(const.SQL_ADD_MODEL_PRED, 
                                      value_list, params={"name": self.name})
            #print(f"db insert {time.time() - start}")
            
        db.execute_sql_query(const.SQL_RENAME_ROAD_TYPE_PRED, self.query_params)
