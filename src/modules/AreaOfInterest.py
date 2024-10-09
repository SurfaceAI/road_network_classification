import os
import sys
from pathlib import Path

import pandas as pd

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
import utils
import constants as const

from tqdm import tqdm


class AreaOfInterest:
    """The area of interest, defined by a bounding box, that the surface is classified for."""

    def __init__(self, config):
        """
        Initializes an AreaOfInterest instance.

        Args:
            config (dict): Configuration dictionary containing the following keys:
                - name (str): The name of the area of interest (aoi).
                - data_path (str): The path where data for this aoi is stored.
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
                - pred_path (str): Path to the model prediction output.
                - road_type_pred_path (str): Path to the road type prediction output.
        """

        # TODO: verify config inputs

        self.name = config["name"]
        self.data_path = os.path.join(config["data_root"], config["name"])
        self.run = config["run"]
        self.minLon = config["minLon"]
        self.minLat = config["minLat"]
        self.maxLon = config["maxLon"]
        self.maxLat = config["maxLat"]
        self.proj_crs = config["proj_crs"]

        # img variables
        self.img_size = config["img_size"]
        self.userid = (
            False if "userid" not in config.keys() else config["userid"]
        )  # only limited to a specific user id? TODO: implement
        self.use_pano = (
            False if "use_pano" not in config.keys() else config["use_pano"]
        )  # exclude panoramic images
        self.dist_from_road = config["dist_from_road"]

        # road network variables
        self.min_road_length = config["min_road_length"]
        self.segment_length = config["segment_length"]
        self.segments_per_group = config["segments_per_group"]

        # customizations
        self.additional_id_column = (
            None
            if "additional_id_column" not in config.keys()
            else config["additional_id_column"]
        )
        self.custom_sql_way_selection = (
            False
            if "custom_sql_way_selection" not in config.keys()
            else config["custom_sql_way_selection"]
        )
        self.custom_road_type_join = (
            False
            if "custom_road_type_join" not in config.keys()
            else config["custom_road_type_join"]
        )
        self.custom_attrs = (
            {} if "custom_attrs" not in config.keys() else config["custom_attrs"]
        )
        self.custom_road_type_separation = (
            False
            if "custom_road_type_separation" not in config.keys()
            else config["custom_road_type_separation"]
        )

        self.img_metadata_path = os.path.join(self.data_path, "img_metadata.csv")
        # model results paths
        self.pred_path = config["pred_path"]
        self.road_type_pred_path = config["road_type_pred_path"]

        self.query_params = self._get_query_params()
        self.query_files = self._get_sql_query_files()

    def remove_img_metadata_file(self):
        os.remove(self.img_metadata_path)
        self.img_metadata_path = None

    def _get_query_params(self):
        additional_id_column = (
            f"{self.additional_id_column}," if self.additional_id_column != None else ""
        )  # add comma
        grouping_ids = f"{additional_id_column}id,part_id,group_num"
        additional_ways_id_column = (
            f"ways.{additional_id_column}" if additional_id_column != "" else ""
        )

        folder = os.path.join(self.data_path, self.run)
        surface_pred_csv_path = os.path.join(folder, "classification_results.csv")
        road_type_pred_csv_path = os.path.join(folder, "scenery_class_results.csv")

        return {
            "name": self.name,
            "bbox0": self.minLon,
            "bbox1": self.minLat,
            "bbox2": self.maxLon,
            "bbox3": self.maxLat,
            "crs": self.proj_crs,
            "dist_from_road": self.dist_from_road,
            "img_metadata_path": self.img_metadata_path,
            "surface_pred_csv_path": surface_pred_csv_path,
            "road_type_pred_csv_path": road_type_pred_csv_path,
            "additional_id_column": additional_id_column,
            "additional_ways_id_column": additional_ways_id_column,
            "grouping_ids": grouping_ids,
            "segment_length": self.segment_length,
            "segments_per_group": self.segments_per_group,
            "min_road_length": self.min_road_length,
        }

    def _get_sql_query_files(self):
        way_selection_query = (
            const.SQL_WAY_SELECTION
            if not self.custom_sql_way_selection
            else self.custom_sql_way_selection
        )

        add_surface_pred_results = (
            const.SQL_JOIN_MODEL_PRED_DIR
            if self.use_pano
            else const.SQL_JOIN_MODEL_PRED
        )

        add_road_type_pred_results = (
            const.SQL_JOIN_TYPE_PRED
            if not self.custom_road_type_join
            else self.custom_road_type_join
        )

        roadtype_separation = (
            const.SQL_ASSIGN_ROAD_TYPES
            if not self.custom_road_type_separation
            else self.custom_road_type_separation
        )

        return {
            "add_img_metadata_table": const.SQL_IMGS_TO_DB,
            "way_selection": way_selection_query,
            "segment_ways": const.SQL_SEGMENT_WAYS,
            "match_img_roads": const.SQL_MATCH_IMG_ROADS,
            "add_surface_pred_results": add_surface_pred_results,
            "add_road_type_pred_results": add_road_type_pred_results,
            "roadtype_separation": roadtype_separation,
            "aggregate_on_roads_1": const.SQL_AGGREGATE_ON_ROADS.format(1),
            "aggregate_on_roads_2": const.SQL_AGGREGATE_ON_ROADS.format(2),
            "aggregate_on_roads_3": const.SQL_AGGREGATE_ON_ROADS.format(3),
        }

    def get_and_write_img_metadata(self, mi, db): 
        # get all relevant tile ids
        db.execute_sql_query(self.query_files["create_img_metadata_table"], self.query_params)

        tiles = mi.tiles_within_bbox(
            [self.minLon, self.minLat, self.maxLon, self.maxLat], const.ZOOM
        )

        rows = []
        for i in tqdm(range(0, len(tiles))):
            tile = tiles[i]
            header, output = mi.metadata_in_tile(tile)
            if output:
                for row in output:
                    # filter img in bbox
                    if (
                        (row[header.index("lon")] > self.minLon)
                        and (row[header.index("lon")] < self.maxLon)
                        and (row[header.index("lat")] > self.minLat)
                        and (row[header.index("lat")] < self.maxLat)
                    ):
                        if (
                            not self.use_pano
                            and row[header.index("is_pano")] == True
                        ):
                            continue
                        if (
                            self.userid
                            and str(row[header.index("creator_id")]) != self.userid
                        ):
                            continue

                        rows += row

            db.add_rows_to_table(f"{self.name}_img_metadata", rows)
        db.execute_sql_query(self.query_files["add_geom_column"], self.query_params)



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

    def classify_images(self, mi, db):
        img_urls = mi.query_img_urls(
            db.img_ids_from_dbtable(f"{self.name}_img_metadata"), self.img_size
        )

        # for img_url in img_urls:
            # img_data = requests.get(img_url, stream=True).content
            # model.predict(img_data)
        # TODO: bring directly into required format and add to db without writing csv
        self.format_pred_files()

        db.execute_sql_query(self.query_files["add_surface_pred_results"], 
                             self.query_params
                             )
        db.execute_sql_query(
            self.query_files["add_road_type_pred_results"], 
            self.query_params
        )



