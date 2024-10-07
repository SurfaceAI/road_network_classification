import pytest
import os
import sys
from pathlib import Path

root_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_dir))
from src.modules.AreaOfInterest import AreaOfInterest


@pytest.fixture
def aoi():
    # Set up any necessary objects or state before each test
    return AreaOfInterest(
        dict(
            name="test_aoi",
            data_root="/path/to/data",
            run="run1",
            minLon=10,
            minLat=15,
            maxLon=20,
            maxLat=25,
            proj_crs=3035,
            img_size="thumb_2048_url",
            dist_from_road=10,
            min_road_length=10,
            segment_length=20,
            segments_per_group=10,
            pred_path="pred_path",
            road_type_pred_path="road_type_pred_path",
        )
    )


def test_initialization(aoi):
    # Test the initialization of the AreaOfInterest object
    assert aoi.name == "test_aoi"
    assert aoi.data_path == "/path/to/data/test_aoi"
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
    assert aoi.segments_per_group == 10


# def test_remove_img_metadata_file(aoi, mocker):
#     # Test the remove_img_metadata_file method
#     # Assuming the method removes a file at self.img_metadata_path
#     aoi.img_metadata_path = "/path/to/img_metadata.csv"

#     # Mock os.remove to avoid actually deleting any files
#     mocker.patch("os.remove")

#     aoi.remove_img_metadata_file()

#     # Check if os.remove was called with the correct path
#     os.remove.assert_called_once_with(aoi.img_metadata_path)

