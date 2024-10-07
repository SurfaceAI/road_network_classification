import os
import sys
from pathlib import Path
import subprocess
import logging

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
import utils
import constants as const


class SurfaceDatabase:
    def __init__(
        self, dbname, dbuser, dbhost, dbpassword, pbf_path, alt_road_network=None
    ):
        self.dbname: str = dbname
        self.dbuser: str = dbuser
        self.dbhost: str = dbhost
        self.dbpassword: str = dbpassword
        self.pbf_path: str = pbf_path
        self.alt_road_network: str = alt_road_network

        self.setup_database(dbname, pbf_path, alt_road_network)

    def __repr__(self):
        return f"Database(name={self.dbname}, tables={self.get_table_names()}, input_road_network={self.pbf_path})"

    def get_table_names(self):
        # TODO
        return []

    def setup_database(self, dbname, pbf_path, alt_road_network=None):
        res = subprocess.run(
            r"psql -lqt | cut -d \| -f 1 | grep -w " + dbname,
            shell=True,
            executable="/bin/bash",
        )
        if res.returncode == 0:
            logging.info(f"database already exists. Skip DB creation step.")
        else:
            logging.info(
                "setup database. Depending on the pbf_file size this might take a while"
            )
            osmosis_scheme_file = os.path.join(
                Path(os.path.dirname(__file__)).parent, "pgsnapshot_schema_0.6.sql"
            )
            subprocess.run(f"createdb {dbname}", shell=True, executable="/bin/bash")
            subprocess.run(
                f"psql  -d {dbname} -c 'CREATE EXTENSION postgis;'",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"psql  -d {dbname} -c 'CREATE EXTENSION hstore;'",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"psql -d {dbname} -f {osmosis_scheme_file}",
                shell=True,
                executable="/bin/bash",
            )
            subprocess.run(
                f"""osmosis --read-pbf {pbf_path} --tf accept-ways 'highway=*' --used-node --tf reject-relations --log-progress --write-pgsql database={dbname}""",
                shell=True,
                executable="/bin/bash",
            )
            logging.info("database setup complete")

        # TODO: implement alt_road_network alternative

    def create_dbconnection(self):
        return psycopg2.connect(
            dbname=self.dbname,
            user=self.dbuser,
            host=self.dbhost,
            password=self.dbpassword,
        )

    def execute_sql_query(self, query, params, is_file=True):
        # create table with sample data points
        conn = self.create_dbconnection()

        if is_file:
            with open(query, "r") as file:
                query = file.read()
        with conn.cursor(cursor_factory=DictCursor) as cursor:
            cursor.execute(sql.SQL(query.format(**params)))
            conn.commit()

        conn.close()

    # TODO: fix function
    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = {table_name});"

        try:
            self.execute_sql_query(query, {"table_name": table_name}, is_file=False)
            return True
        except Exception as e:
            return False

    def table_to_shapefile(self, table_name, output_file):
        subprocess.run(
            f'pgsql2shp -f "{output_file}" {self.dbname} "select * from {table_name}"',
            shell=True,
            executable="/bin/bash",
        )
        logging.info(f"table {table_name} writtena as shp file to {output_file}")

    def add_img_metadata_table(self, aoi, mapillary_interface):
        mapillary_interface.query_metadata(aoi)

        # TODO: check if table already exists and warn that this will be overwritten
        # if self.table_exists(f"{aoi.name}_img_metadata"):
        #    logging.info(f"img table for {aoi.name} already exists. Overwriting table")

        logging.info(f"create img table {aoi.name} in database {self.dbname}")

        query_file = os.path.join(const.SQL_IMGS_TO_DB)
        self.execute_sql_query(
            query_file,
            {
                "table_name": f"{aoi.name}_img_metadata",
                "absolute_path": aoi.img_metadata_path,
            },
        )

        aoi.remove_img_metadata_file()

    def preprocess_line_segments(self, aoi):
        logging.info(f"create linestrings in bounding box")
        query_file = (
            const.SQL_WAY_SELECTION
            if not aoi.custom_sql_way_selection
            else aoi.custom_sql_way_selection
        )
        query_file = os.path.join(query_file)
        additional_id_column = (
            f"{aoi.additional_id_column}," if aoi.additional_id_column != None else ""
        )  # add comma

        # TODO: This step takes quite long for OSM if bbox is large - possible to speed up?
        self.execute_sql_query(
            query_file,
            {
                "bbox0": aoi.minLon,
                "bbox1": aoi.minLat,
                "bbox2": aoi.maxLon,
                "bbox3": aoi.maxLat,
                "table_name_way_selection": f"{aoi.name}_way_selection",
                **aoi.custom_attrs,
            },
        )

        logging.info(f"cut lines into segments of length {aoi.segment_length}")
        query_file = os.path.join(const.SQL_SEGMENT_WAYS)
        self.execute_sql_query(
            query_file,
            {
                "name": aoi.name,
                "segment_length": aoi.segment_length,
                "min_road_length": aoi.min_road_length,
                "additional_id_column": additional_id_column,
            },
        )
        logging.info("line preparation complete")

    def img_ids_from_dbtable(self, db_table):
        conn = self.create_dbconnection()

        with conn.cursor(cursor_factory=DictCursor) as cursor:
            img_ids = cursor.execute(sql.SQL(f"SELECT img_id FROM {db_table}"))
            img_ids = cursor.fetchall()
            img_ids = [img_id[0] for img_id in img_ids]
        conn.close()
        return img_ids

    def img_ids_to_csv(self, data_path, db_table, file_name):
        ids = self.img_ids_from_dbtable(db_table)
        pd.DataFrame({"img_id": ids}).to_csv(
            os.path.join(data_path, file_name), index=False
        )
        logging.info(f"img ids written to {os.path.join(data_path, file_name)}")

    def match_imgs_to_segments(self, aoi):
        # TODO: catch if img db or segment db not created yet

        logging.info("match imgs to road linestrings")

        query_file = const.SQL_MATCH_IMG_ROADS
        self.execute_sql_query(
            query_file,
            {
                "name": aoi.name,
                "dist_from_road": aoi.dist_from_road,
                "crs": aoi.proj_crs,
            },
        )
        logging.info("matching complete")

    def add_model_results(self, aoi):
        logging.info("add surface classification results to db")
        pred = utils.format_predictions(
            pd.read_csv(aoi.pred_path, dtype={"Image": str, "Level_1": str}),
            is_pano=aoi.use_pano,
        )
        folder = os.path.join(aoi.data_path, aoi.run)
        csv_path = os.path.join(folder, "classification_results.csv")
        os.makedirs(folder, exist_ok=True)
        pred.to_csv(csv_path, index=False)
        logging.info(f"{csv_path} written")

        query_file = (
            const.SQL_JOIN_MODEL_PRED_DIR if aoi.use_pano else const.SQL_JOIN_MODEL_PRED
        )

        self.execute_sql_query(
            query_file,
            {
                "table_name_point_selection": f"{aoi.name}_img_metadata",
                "csv_path": csv_path,
            },
        )

        query_file = (
            const.SQL_JOIN_TYPE_PRED
            if not aoi.custom_road_type_join
            else aoi.custom_road_type_join
        )

        if aoi.road_type_pred_path:
            logging.info("add scenery results to db")
            pred_scene = utils.format_scenery_predictions(
                pd.read_csv(aoi.road_type_pred_path, dtype={"Image": str}),
                is_pano=aoi.use_pano,
            )
            csv_path_scenery = os.path.join(
                aoi.data_path, aoi.run, "scenery_class_results.csv"
            )
            pred_scene.to_csv(csv_path_scenery, index=False)

            self.execute_sql_query(
                query_file,
                {
                    "table_name_point_selection": f"{aoi.name}_img_metadata",
                    "csv_path": csv_path_scenery,
                },
            )

        logging.info("completed adding classification results to db")

    def roadtype_separation(self, aoi):
        query_file = (
            const.SQL_ASSIGN_ROAD_TYPES
            if not aoi.custom_road_type_separation
            else aoi.custom_road_type_separation
        )
        if query_file != None:
            logging.info(
                "create partitions for each road type of a geometry in a separate table"
            )

            self.execute_sql_query(query_file, {"name": aoi.name})
            logging.info("road type separation complete")
        else:
            logging.info("no road type separation needed")

    def aggregate_by_road_segment(self, aoi):
        logging.info("aggregate by road segment")
        additional_id_column = (
            f"{aoi.additional_id_column}," if aoi.additional_id_column != None else ""
        )  # add comma
        grouping_ids = f"{additional_id_column}id,part_id,group_num"
        additional_id_column = (
            f"ways.{additional_id_column}" if additional_id_column != "" else ""
        )
        params = {
            "name": aoi.name,
            "additional_id_column": additional_id_column,
            "grouping_ids": grouping_ids,
            "table_name_point_selection": f"{aoi.name}_img_metadata",
            "segments_per_group": aoi.segments_per_group,
            "min_road_length": aoi.min_road_length,
        }

        query_file = const.SQL_AGGREGATE_ON_ROADS.format(1)
        self.execute_sql_query(query_file, params)

        query_file = const.SQL_AGGREGATE_ON_ROADS.format(2)
        self.execute_sql_query(query_file, params)

        query_file = const.SQL_AGGREGATE_ON_ROADS.format(3)
        self.execute_sql_query(query_file, params)

        logging.info("aggregation complete")

    def process_area_of_interest(self, area_of_interest, mapillary_interface):
        self.add_img_metadata_table(area_of_interest, mapillary_interface)

        self.preprocess_line_segments(area_of_interest)

        self.match_imgs_to_segments(area_of_interest)

        ##### classify images
        # TODO: parallelize download and classification step?
        # mapillary_interface.download_imgs_from_table(
        #                 os.path.join(area_of_interest.data_path, area_of_interest.run, "imgs"),
        #                 area_of_interest.img_size,
        #                 database=self,
        #                 db_table=f"{area_of_interest.name}_img_metadata")

        # TODO: include classification model into pipeline
        # self.classify()

        self.add_model_results(area_of_interest)

        #### ---- aggregation algorithm ---- ####
        # create table with road partitions (i.e., each road type for each geoemtry)
        self.roadtype_separation(area_of_interest)

        self.aggregate_by_road_segment(area_of_interest)

        output_file = os.path.join(
            area_of_interest.data_path,
            area_of_interest.run,
            f"{area_of_interest.name}_pred.shp",
        )
        self.table_to_shapefile(
            f"{area_of_interest.name}_group_predictions", output_file
        )
