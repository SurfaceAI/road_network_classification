import os
import logging

# local modules
#import utils

class AreaOfInterest:
    def __init__(self, config):

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
        self.userid = False if "userid" not in config.keys() else config["userid"] # only limited to a specific user id? TODO: implement
        self.use_pano = False if "use_pano" not in config.keys() else config["use_pano"] # exclude panoramic images
        self.dist_from_road = config["dist_from_road"]

        # road network variables
        self.min_road_length = config["min_road_length"]
        self.segment_length = config["segment_length"]
        self.segments_per_group = config["segments_per_group"]

        # customizations
        self.additional_id_column = None if "additional_id_column" not in config.keys() else config["additional_id_column"]
        self.custom_sql_way_selection = False if "custom_sql_way_selection" not in config.keys() else config["custom_sql_way_selection"]
        self.custom_road_type_join = False if "custom_road_type_join" not in config.keys() else config["custom_road_type_join"]
        self.custom_attrs = {} if "custom_attrs" not in config.keys() else config["custom_attrs"]
        self.custom_road_type_separation = False if "custom_road_type_separation" not in config.keys() else config["custom_road_type_separation"]

        # model results paths
        self.pred_path = config["pred_path"]
        self.road_type_pred_path = config["road_type_pred_path"]


    def set_img_metadata_path(self):
        self.img_metadata_path =  os.path.join(self.data_path, "img_metadata.csv")
        return self.img_metadata_path

    def remove_img_metadata_file(self):
        os.remove(self.img_metadata_path)
        self.img_metadata_path = None

    