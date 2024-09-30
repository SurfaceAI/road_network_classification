import os
from pathlib import Path
import subprocess
import logging
import constants as const
import utils
import pandas as pd

class SurfaceDatabase:
    def __init__(self, dbname, pbf_path, alt_road_network=None):
        self.dbname:str = dbname
        self.tables:list = []
        self.pbf_path: str = pbf_path
        self.alt_road_network:str = alt_road_network

        self.setup_database(dbname, pbf_path, alt_road_network)



    def __repr__(self):
        return f"Database(name={self.dbname}, tables={self.tables}, input_road_network={self.pbf_path})"
    

    def setup_database(self, dbname, pbf_path, alt_road_network=None):
        res = subprocess.run(r"psql -lqt | cut -d \| -f 1 | grep -w " + dbname, shell = True, executable="/bin/bash")
        if res.returncode == 0:
            logging.info(f"database already exists. Skip DB creation step.")
        else:
            logging.info("setup database. Depending on the pbf_file size this might take a while")
            osmosis_scheme_file = os.path.join(Path(os.path.dirname(__file__)).parent, 'pgsnapshot_schema_0.6.sql')
            subprocess.run(f"createdb {dbname}", shell = True, executable="/bin/bash")
            subprocess.run(f"psql  -d {dbname} -c 'CREATE EXTENSION postgis;'", shell = True, executable="/bin/bash")
            subprocess.run(f"psql  -d {dbname} -c 'CREATE EXTENSION hstore;'", shell = True, executable="/bin/bash")
            subprocess.run(f"psql -d {dbname} -f {osmosis_scheme_file}", shell = True, executable="/bin/bash")
            subprocess.run(f"""osmosis --read-pbf {pbf_path} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={dbname}""", shell = True, executable="/bin/bash")
            logging.info("database setup complete")

        # TODO: update self.tables
        
        # TODO: implement alt_road_network alternative


    # TODO: fix function
    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {table_name});"
        
        try:
            utils.execute_sql_query(self.dbname, query, {"table_name": table_name}, is_file=False)
            return True
        except Exception as e:
            return False



    def add_img_metadata_table(self, aoi, mapillary_interface):
        
        mapillary_interface.query_metadata(aoi)

        
        # TODO: check if table already exists and warn that this will be overwritten
        #if self.table_exists(f"{aoi.name}_img_metadata"):
        #    logging.info(f"img table for {aoi.name} already exists. Overwriting table")

        logging.info(f"create img table {aoi.name} in database {self.dbname}")

        query_file = os.path.join(const.SQL_IMGS_TO_DB)
        utils.execute_sql_query(self.dbname, query_file,
                        {"table_name": f"{aoi.name}_img_metadata", 
                        "absolute_path": aoi.img_metadata_path})

        aoi.remove_img_metadata_file()
        # TODO: update self.tables


    def preprocess_line_segments(self, aoi):

        logging.info(f"create linestrings in bounding box")
        query_file = const.SQL_WAY_SELECTION if not aoi.custom_sql_way_selection else aoi.custom_sql_way_selection
        query_file = os.path.join(query_file)
        additional_id_column = f"{aoi.additional_id_column}," if aoi.additional_id_column != None else "" # add comma

        # TODO: This step takes quite long for OSM if bbox is large - possible to speed up?
        utils.execute_sql_query(self.dbname, query_file, 
                            {"bbox0": aoi.minLon, "bbox1": aoi.minLat, "bbox2": 
                             aoi.maxLon, "bbox3": aoi.maxLat,
                        "table_name_way_selection": f"{aoi.name}_way_selection", **aoi.custom_attrs
                        })

        logging.info(f"cut lines into segments of length {aoi.segment_length}")
        query_file = os.path.join(const.SQL_SEGMENT_WAYS)
        utils.execute_sql_query(self.dbname, query_file, 
                        {"name": aoi.name,
                        "segment_length": aoi.segment_length,
                        "min_road_length": aoi.min_road_length,
                        "additional_id_column": additional_id_column
                        })
        logging.info("line preparation complete")


    def match_imgs_to_segments(self,aoi):
        
        # TODO: catch if img db or segment db not created yet
        
        logging.info("match imgs to road linestrings")

        query_file = const.SQL_MATCH_IMG_ROADS
        utils.execute_sql_query(self.dbname, query_file, 
                        {"name": aoi.name,
                        "dist_from_road": aoi.dist_from_road,
                        "crs": aoi.proj_crs})
        logging.info("matching complete")


    def add_model_results(self, aoi):

        logging.info("add surface classification results to db")
        pred = utils.format_predictions(pd.read_csv(aoi.pred_path, dtype={"Image": str, "Level_1":str}), pano=aoi.pano)
        folder = os.path.join(aoi.data_path, aoi.run)
        csv_path = os.path.join(folder, "classification_results.csv")
        os.makedirs(folder, exist_ok=True)
        pred.to_csv(csv_path, index=False)
        logging.info(f"{csv_path} written")

        query_file = const.SQL_JOIN_MODEL_PRED_DIR if aoi.pano else const.SQL_JOIN_MODEL_PRED

        utils.execute_sql_query(self.dbname, query_file, 
                        {"table_name_point_selection": f"{aoi.name}_img_metadata",
                        "csv_path": csv_path,
                        "pano": aoi.pano})

        query_file = const.SQL_JOIN_SCENERY_PRED if not aoi.custom_road_scenery_join else aoi.custom_road_scenery_join

        if aoi.road_scenery_path:
            logging.info("add scenery results to db")
            pred_scene = utils.format_scenery_predictions(pd.read_csv(aoi.road_scenery_path, dtype={"Image": str}), pano=aoi.pano)
            csv_path_scenery = os.path.join(aoi.data_path, aoi.run, "scenery_class_results.csv")
            pred_scene.to_csv(csv_path_scenery, index=False)

            utils.execute_sql_query(self.dbname, query_file, 
                        {"table_name_point_selection": f"{aoi.name}_img_metadata",
                        "csv_path": csv_path_scenery,
                        "pano": aoi.pano})

        logging.info("completed adding classification results to db")
