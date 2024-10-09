import os
import sys
from pathlib import Path
import pytest
import mercantile
from unittest.mock import MagicMock, call, mock_open
import requests

root_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_dir))
from src.modules.MapillaryInterface import MapillaryInterface
from src.modules.AreaOfInterest import AreaOfInterest
import csv


@pytest.fixture
def mapillary_interface():
    return MapillaryInterface(
        mapillary_token="MLY|MAPILLARY_TOKEN", parallel=True, parallel_batch_size=10
    )


@pytest.fixture
def aoi():
    # Set up any necessary objects or state before each test
    return AreaOfInterest(
        dict(
            name="test_aoi",
            run="run1",
            minLon=12.01,
            minLat=50.01,
            maxLon=12.15,
            maxLat=50.015,
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


def test_tiles_within_bbox(mapillary_interface):
    minLon, minLat, maxLon, maxLat = 12.01, 50.01, 12.02, 50.015
    tiles = mapillary_interface.tiles_within_bbox([minLon, minLat, maxLon, maxLat], 14)
    assert tiles == [mercantile.Tile(8738, 5555, 14), mercantile.Tile(8739, 5555, 14)]


def test_tiles_within_boundary(mapillary_interface):
    pass



# def test_download_image(mapillary_interface, mocker):
#     mock_response = MagicMock(status_code=200, content=b"binary image data 100")
#     mock_response.json.return_value = {"thumb_2048_url": "https://IMG_URL"}

#     mocked_response = mocker.patch("requests.get", return_value=mock_response)

#     mocked_open = mocker.patch("builtins.open", mock_open())

#     download_path = "/path/to/download"

#     # Call the method to test
#     mapillary_interface.download_image("100", "thumb_2048_url", download_path)

#     # Verify that the requests.get method was called with the correct URLs
#     expected_calls = [
#         call(
#             "https://graph.mapillary.com/100",
#             params={"fields": "thumb_2048_url", "access_token": "MLY|MAPILLARY_TOKEN"},
#         ),
#         call("https://IMG_URL", stream=True),
#     ]
#     mocked_response.assert_has_calls(expected_calls, any_order=True)

#     # Verify that the open method was called with the correct file paths
#     expected_calls = [
#         call(os.path.join(download_path, "100.jpg"), "wb"),
#         call().write(b"binary image data 100"),
#     ]
#     mocked_open.assert_has_calls(expected_calls, any_order=True)
