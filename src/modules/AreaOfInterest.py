import os


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

        # model results paths
        self.pred_path = config["pred_path"]
        self.road_type_pred_path = config["road_type_pred_path"]

    def set_img_metadata_path(self):
        self.img_metadata_path = os.path.join(self.data_path, "img_metadata.csv")
        return self.img_metadata_path

    def remove_img_metadata_file(self):
        os.remove(self.img_metadata_path)
        self.img_metadata_path = None
