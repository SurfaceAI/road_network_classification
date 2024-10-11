import pytest
import os
import sys
from pathlib import Path
from PIL import Image

root_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_dir))
from src.modules.Models import ModelInterface


@pytest.fixture
def model_interface():
    # Set up any necessary objects or state before each test
    return ModelInterface(
        dict(
            model_root="src/models",
            models={
                "surface_type": "surface-efficientNetV2SLinear-20240923_171219-2t59l5b9_epoch10.pt",
                "surface_quality": {
                    "asphalt": "smoothness-asphalt-efficientNetV2SLinear-20240923_144409-86vpv5bs_epoch29.pt",
                    "concrete": "smoothness-concrete-efficientNetV2SLinear-20240924_020702-32bp575u_epoch16.pt",
                    "paving_stones": "smoothness-paving_stones-efficientNetV2SLinear-20240924_035145-0u6eheod_epoch18.pt",
                    "sett": "smoothness-sett-efficientNetV2SLinear-20240924_103221-xrpbxnjc_epoch26.pt",
                    "unpaved": "smoothness-unpaved-efficientNetV2SLinear-20240924_131624-qfvm272n_epoch21.pt"
                },
                "road_type": "flatten-efficientNetV2SLinear-20240917_125206-9lg7mdeu_epoch10.pt"
            },
            gpu_kernel=0,
            transform_surface={
                "resize": 384,
                "crop": "lower_middle_half"
            },
            transform_road_type={
                "resize": 384,
                "crop": "lower_half"
            },
            batch_size=48,
        )
    )


# def test_initialization(model_interface):
#     # Test the initialization of the AreaOfInterest object
#     pass
#     # assert aoi.name == "test_aoi"
#     # assert aoi.run == "run1"
#     # assert aoi.minLon == 10
#     # assert aoi.minLat == 15
#     # assert aoi.maxLon == 20
#     # assert aoi.maxLat == 25
#     # assert aoi.proj_crs == 3035
#     # assert aoi.img_size == "thumb_2048_url"
#     # assert aoi.dist_from_road == 10
#     # assert aoi.min_road_length == 10
#     # assert aoi.segment_length == 20
#     # assert aoi.segments_per_group == None


def test_model_predict(model_interface):
    input_data = []
    for image_id in ["1000068877331935", "458670231871080", "1000140361462393"]:
        image_path=os.path.join(root_dir, "tests", "test_data", f"{image_id}.jpg")
        input_data.append(Image.open(image_path))

    

     # TODO: store example image in test_data and read it here
    expected_output= [
        ['1_1_road__1_1_road_general', round(0.9949304461479187, 5), 'asphalt', round(0.99986732006073, 5), round(2.195223569869995, 5)],
        ['1_3_pedestrian__1_3_footway', round(0.9967668056488037, 5), 'paving_stones', round(0.9456618428230286, 5), round(4.516800880432129, 5)],
        ['1_1_road__1_1_road_general', round(0.9890756607055664, 5), 'asphalt', round(0.9999390840530396, 5), round(1.8645464181900024, 5)],
    ]

    output = model_interface.batch_classifications(input_data)
    assert output == expected_output

