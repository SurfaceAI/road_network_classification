import os
import sys
from pathlib import Path
import pytest
from unittest.mock import MagicMock, call
import numpy as np

root_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_dir))
from src.modules.AreaOfInterest import AreaOfInterest
from src import constants as const


@pytest.fixture
def aoi():
    # Set up any necessary objects or state before each test
    return AreaOfInterest(
        dict(
            name="test_aoi",
            run="run1",
            minLon=10.01,
            minLat=15.01,
            maxLon=10.02,
            maxLat=15.02,
            proj_crs=3035,
            img_size="thumb_2048_url",
            dist_from_road=10,
            min_road_length=10,
            segment_length=20,
            segments_per_group=None,
            pred_path="pred_path",
            road_type_pred_path="road_type_pred_path",
        )
    )


def test_initialization(aoi):
    # Test the initialization of the AreaOfInterest object
    assert aoi.name == "test_aoi"
    assert aoi.run == "run1"
    assert aoi.minLon == 10
    assert aoi.minLat == 15
    assert aoi.maxLon == 20
    assert aoi.maxLat == 25
    assert aoi.proj_crs == 3035
    assert aoi.img_size == "thumb_2048_url"
    assert aoi.dist_from_road == 10
    assert aoi.min_road_length == 10
    assert aoi.segment_length == 20
    assert aoi.segments_per_group == None


def test__get_query_params(aoi):
    # Test the _get_query_params method of the AreaOfInterest object
    assert aoi._get_query_params() == {
        "name": "test_aoi",
        "bbox0": 10,
        "bbox1": 15,
        "bbox2": 20,
        "bbox3": 25,
        "crs": 3035,
        "dist_from_road": 10,
        "additional_id_column": "",
        "additional_ways_id_column": "",
        "grouping_ids": "id,part_id,group_num",
        "segment_length": 20,
        "group_num": "0",
        "segments_per_group": None,
        "min_road_length": 10,
    }


def test_get_and_write_img_metadata(aoi, mocker):
    mock_mi = MagicMock()
    header = [
        "img_id",
        "sequence_id",
        "captured_at",
        "compass_angle",
        "is_pano",
        "creator_id",
        "lon",
        "lat",
    ]
    output = [
        ["001", "101", "1234", "90", "False", "00001", "10.01", "15.01"],
        ["002", "101", "1235", "90", "False", "00001", "10.011", "15.011"],
    ]
    mock_mi.metadata_in_tile = MagicMock(return_value=(header, output))

    mock_db = MagicMock()
    mock_db.execute_sql_query = MagicMock()
    mock_db.add_rows_to_table = MagicMock()

    aoi.get_and_write_img_metadata(mock_mi, mock_db)

    mock_mi.metadata_in_tile.assert_called()
    assert mock_db.add_rows_to_table.call_args[0][0] == "test_aoi_img_metadata"
    assert mock_db.add_rows_to_table.call_args[0][1] == header
    assert np.all(np.array(output) == mock_db.add_rows_to_table.call_args[0][2])
    mock_db.add_rows_to_table.assert_called()
    mock_db.execute_sql_query.assert_has_calls(
        [
            call(const.SQL_CREATE_IMG_METADATA_TABLE, aoi.query_params),
            call(const.SQL_ADD_GEOM_COLUMN, aoi.query_params),
        ]
    )


def test_classify_images(aoi):
    mock_mi = MagicMock()
    mock_mi.query_imgs = MagicMock()
    mock_db = MagicMock()
    mock_db.img_ids_from_dbtable = MagicMock(return_value=["001", "002"])
    mock_db.table_exists = MagicMock(return_value=False)
    mock_db.execute_sql_query = MagicMock()
    mock_db.add_rows_to_table = MagicMock()
    mock_md = MagicMock(batch_size=48)
    md_output = [
        [
            "1_1_road__1_1_road_general",
            round(0.9949304461479187, 5),
            "asphalt",
            round(0.99986732006073, 5),
            round(2.195223569869995, 5),
        ],
        [
            "1_3_pedestrian__1_3_footway",
            round(0.9967668056488037, 5),
            "paving_stones",
            round(0.9456618428230286, 5),
            round(4.516800880432129, 5),
        ],
    ]
    value_list = [[img_id] + mo for img_id, mo in zip(["001", "002"], md_output)]
    mock_md.batch_classifications = MagicMock(return_value=md_output)

    aoi.classify_images(mock_mi, mock_db, mock_md)
    mock_db.img_ids_from_dbtable.assert_called_once()
    assert mock_db.add_rows_to_table.call_args[0][0] == "test_aoi_img_classifications"
    assert mock_db.add_rows_to_table.call_args[0][1] == [
        "img_id",
        "road_type_pred",
        "road_type_prob",
        "type_pred",
        "type_class_prob",
        "quality_pred",
    ]
    assert np.all(np.array(value_list) == mock_db.add_rows_to_table.call_args[0][2])
    mock_db.add_rows_to_table.assert_called_once()
    mock_db.execute_sql_query.assert_has_calls(
        [
            call(const.SQL_PREP_MODEL_RESULT, aoi.query_params),
            call(const.SQL_RENAME_ROAD_TYPE_PRED, aoi.query_params),
        ]
    )
