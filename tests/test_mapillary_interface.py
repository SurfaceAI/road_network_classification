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
            data_root=os.path.join(root_dir, "tests", "test_data"),
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


def test_query_metadata(mapillary_interface, aoi, mocker):
    # Mock the API call to return a predefined response
    mock_response = MagicMock()
    with open(
        os.path.join(
            root_dir, "tests", "test_data", "test_aoi", "mly1_public-2-14-8738-5555.pbf"
        ),
        "rb",
    ) as file:
        mock_response.content = file.read()

    mock_response.status_code = 200
    mocker.patch(
        "src.modules.MapillaryInterface.requests.get", return_value=mock_response
    )

    mapillary_interface.query_metadata(aoi)

    with open(aoi.img_metadata_path, mode="r") as file:
        reader = csv.reader(file)
        data = [row for row in reader]

    assert data[0:2] == [
        [
            "img_id",
            "tile_id",
            "sequence_id",
            "captured_at",
            "compass_angle",
            "is_pano",
            "creator_id",
            "lon",
            "lat",
        ],
        [
            "2155254798207173",
            "8739_5555_14",
            "Q59taBo0UGIAxmYrC1q7yc",
            "1722615253554",
            "188.0",
            "False",
            "105373491703247",
            "12.023001909255981",
            "50.013158377703746",
        ],
    ]

    aoi.remove_img_metadata_file()


def test_download_image(mapillary_interface, mocker):
    mock_response = MagicMock(status_code=200, content=b"binary image data 100")
    mock_response.json.return_value = {"thumb_2048_url": "https://IMG_URL"}

    mocked_response = mocker.patch("requests.get", return_value=mock_response)

    mocked_open = mocker.patch("builtins.open", mock_open())

    download_path = "/path/to/download"

    # Call the method to test
    mapillary_interface.download_image("100", "thumb_2048_url", download_path)

    # Verify that the requests.get method was called with the correct URLs
    expected_calls = [
        call(
            "https://graph.mapillary.com/100",
            params={"fields": "thumb_2048_url", "access_token": "MLY|MAPILLARY_TOKEN"},
        ),
        call("https://IMG_URL", stream=True),
    ]
    mocked_response.assert_has_calls(expected_calls, any_order=True)

    # Verify that the open method was called with the correct file paths
    expected_calls = [
        call(os.path.join(download_path, "100.jpg"), "wb"),
        call().write(b"binary image data 100"),
    ]
    mocked_open.assert_has_calls(expected_calls, any_order=True)
