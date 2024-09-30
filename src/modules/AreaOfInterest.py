import os
import utils
import logging

class AreaOfInterest:
    def __init__(self, config):
        
        self.name = config["name"]
        self.data_path = config["data_path"]
        self.run = config["run"]
        self.minLon = config["minLon"]
        self.minLat = config["minLat"]
        self.maxLon = config["maxLon"]
        self.maxLat = config["maxLat"]
        self.proj_crs = config["proj_crs"]

        # img variables
        self.img_size = config["img_size"]
        self.userid = False if "userid" not in config.keys() else config["userid"] # only limited to a specific user id? TODO: implement
        self.no_pano = True if "no_pano" not in config.keys() else config["no_pano"] # exclude panoramic images
        self.dist_from_road = config["dist_from_road"]

        # road network variables
        self.min_road_length = config["min_road_length"]
        self.segment_length = config["segment_length"]

        # customizations
        self.additional_id_column = None if "additional_id_column" not in config.keys() else config["additional_id_column"]
        self.custom_sql_way_selection = False if "custom_sql_way_selection" not in config.keys() else config["custom_sql_way_selection"]
        self.custom_road_scenery_join = False if "custom_road_scenery_join" not in config.keys() else config["custom_road_scenery_join"]
        self.custom_attrs = {} if "custom_attrs" not in config.keys() else config["custom_attrs"]
        
        # model results paths
        self.pred_path = config["pred_path"]
        self.road_type_pred_path = config["road_type_pred_path"]


    def set_img_metadata_path(self):
        self.img_metadata_path =  os.path.join(self.data_path, "img_metadata.csv")
        return self.img_metadata_path

    def remove_img_metadata_file(self):
        os.remove(self.img_metadata_path)
        self.img_metadata_path = None

    def process_area_of_interest(self, database, mapillary_interface):
        ##### ---- get image metadata in bounding box ---- ######
        
        #database.add_img_metadata_table(self, mapillary_interface)

        ##### ---- prepare line segments ---- ######
        database.preprocess_line_segments(self)

        ##### ---- match images to road segments ---- ######
        database.match_imgs_to_segments(self)

        ##### ---- classify images ---- ######
        # TODO: parallelize download and classification step?
        self.img_download(dbname=database.database, 
                          db_table=f"{self.name}_img_metadata")
        
        database.add_model_results(self)
        

    

    def img_download(self,
                        mapillary_interface,
                        dbname,
                        dest_folder_name = "imgs",
                        csv_path = None, db_table = None, img_id_col=False):

        if csv_path:
            img_id_col = 1 if (img_id_col is False) else img_id_col
            img_ids = utils.img_ids_from_csv(csv_path, img_id_col=img_id_col)
        elif db_table:
            img_ids = utils.img_ids_from_dbtable(db_table, dbname)


        # only download images that are not present yet in download folder
        download_folder = os.path.join(self.data_path, self.run, dest_folder_name)
        if os.path.exists(download_folder):
            imgs_in_download_folder =  os.listdir(download_folder)
            imgIDs_in_download_folder = [img_id.split(".")[0] for img_id in imgs_in_download_folder]
            img_ids = list(set(img_ids) - set(imgIDs_in_download_folder))

        logging.info(f"Downloading {len(img_ids)} images")
        mapillary_interface.download_images(img_ids, self.img_size, download_folder)

