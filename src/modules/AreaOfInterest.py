import logging
import os
import sys
from pathlib import Path

import mercantile
import numpy as np

# local modules
src_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(src_dir))
from tqdm import tqdm

import constants as const
from pano_utils import pano_to_persp, compute_yaw, compute_direction_of_travel


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
        self.pano_img_size = config.get("pano_img_size", "thumb_2048_url")
        self.userid = config.get(
            "userid", False
        )  # only limited to a specific user id? TODO: implement
        self.use_pano = config.get(
            "use_pano", False
        )  # exclude panoramic images # TODO: implement inclusion
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

    def _get_imgs_to_classify(self, db):
        img_ids = db.cols_from_dbtable(f"{self.name}_img_metadata", ["img_id"],
                                       where=f'is_pano={self.use_pano}')[0]
        
        # get already classified images to exclude from classification
        if db.table_exists(f"{self.name}_img_classifications"):
            existing_img_ids = db.cols_from_dbtable(
                f"{self.name}_img_classifications", ["img_id"]
            )[0]
            logging.info(f"existing classified images: {len(existing_img_ids)}")
        else:
            existing_img_ids = []

        # seperate pano and non-pano images
        if self.use_pano:
            non_pano_img_ids = db.cols_from_dbtable(
                f"{self.name}_img_metadata", ["img_id"], where='is_pano=False'
            )[0]
            pano_img_ids = list(set(img_ids) - set(non_pano_img_ids) - set(existing_img_ids))
            non_pano_img_ids = list(set(non_pano_img_ids) - set(existing_img_ids))
            img_id_dict = {"non_pano": non_pano_img_ids, "pano": pano_img_ids}
        else:
            img_ids = list(set(img_ids) - set(existing_img_ids))
            img_id_dict = {"non_pano": img_ids}
        return img_id_dict
    
    def _compute_yaws(self, db, img_ids):
        # compute yaw for each sequence
        formatted_img_ids = [f"'{img_id}'" for img_id in img_ids]
        metadata = db.cols_from_dbtable(f"{self.name}_img_metadata", 
                                            columns = ["img_id", "compass_angle", "sequence_id"],
                                            where = f"img_id in ({','.join(formatted_img_ids)})"
                                            )
        sequ_img_ids = metadata[0]
        compass_angles = metadata[1]
        sequence_ids = metadata[2]
        img_to_seq = dict(zip(sequ_img_ids, sequence_ids))
        yaws_per_sequence = dict()

        # compute yaws for 5 images of each per sequence and average
        for sequence_id in np.unique(sequence_ids):
            seq_yaws = []
            for j in [6, 4, 2, 1.8, 1.2]:
                sequ_id_ids = [i for i, x in enumerate(sequence_ids) if x == sequence_id]
                sequ_id_id = sequ_id_ids[int(len(sequ_id_ids)/j)] 
                sequ_img_id = sequ_img_ids[sequ_id_id]
                neighbor_coords = db.execute_sql_query(const.SQL_GET_SEQUENCE_NEIGHBORS, 
                                    {**self.query_params, "img_id": sequ_img_id, "sequence_id": sequence_id}, get_response=True)
                seq_yaws.append(compute_yaw(compass_angles[sequ_id_id], compute_direction_of_travel(neighbor_coords)))
            yaws_per_sequence[sequence_id]= np.mean(seq_yaws)
        
        return [yaws_per_sequence[img_to_seq[i]] for i in sequ_img_ids]

    def classify_images(self, mi, db, md):
        db.execute_sql_query(const.SQL_PREP_MODEL_RESULT, self.query_params)
        img_id_dict = self._get_imgs_to_classify(db)

        for img_type in img_id_dict.keys():
            img_ids = img_id_dict[img_type]
            
            if (img_type == "non_pano") & (len(img_ids) > 0):
                img_size = self.img_size
                batch_size = md.batch_size
                direction = 0
            elif (img_type == "pano") & (len(img_ids) > 0):
                img_size = self.pano_img_size 
                # bc pano images have a front and back, we need to half the batch size
                batch_size = int(md.batch_size / 2)
                yaws = self._compute_yaws(db, img_ids)
    
            for i in tqdm(
                range(0, len(img_ids), batch_size),
                desc=f"Download and classify {len(img_ids)} {img_type} images",
            ):
                j = min(i + batch_size, len(img_ids))

                batch_img_ids = img_ids[i:j]

                img_data = mi.query_imgs(
                    batch_img_ids,
                    img_size,
                )
                if img_type == "pano":
                    img_data = [pano_to_persp(img, yaw, direction) for img, yaw in zip(img_data, yaws) for direction in [0, 1]]
                    batch_img_ids = [f"{img_id}_{direction}" for img_id in batch_img_ids for direction in [0,1]]

                model_output = md.batch_classifications(img_data)

                # add img_id to model_output
                # start = time.time()
                value_list = [
                    [img_id] + mo for img_id, mo in zip(batch_img_ids, model_output)
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

    def imgs_to_shapefile(self, db, output_path):
        query = f"""
        DROP TABLE IF EXISTS temp_imgs;
        SELECT meta.*, cl.road_type_pred, cl.road_type_prob, cl.type_pred, cl.type_class_prob, cl.quality_pred
		INTO TABLE temp_imgs
	    FROM {self.name}_img_metadata meta 
	    JOIN {self.name}_img_classifications cl 
	    ON meta.img_id=cl.img_id;"""
        db.execute_sql_query(query, is_file=False)
        db.table_to_shapefile("temp_imgs", output_path)
        db.execute_sql_query("DROP TABLE temp_imgs;", is_file=False)

    def road_network_with_osm_groundtruth(self, db, output_path):
        db.execute_sql_query(const.SQL_CLEAN_SURFACE, self.query_params)

        query = f"""
        DROP TABLE IF EXISTS temp_rn;
        SELECT gp.*, ws.surface_clean, ws.smoothness
		INTO TABLE temp_rn
	    FROM {self.name}_group_predictions gp 
	    JOIN {self.name}_way_selection ws 
	    ON gp.id=ws.id
        WHERE part_id=1;"""
        db.execute_sql_query(query, is_file=False)
        db.table_to_shapefile("temp_rn", output_path)
        db.execute_sql_query("DROP TABLE temp_rn;", is_file=False)
