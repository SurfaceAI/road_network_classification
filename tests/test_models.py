import os
import sys
from pathlib import Path

import pytest
from PIL import Image

root_dir = Path(os.path.abspath(__file__)).parent.parent
sys.path.append(str(root_dir))
from src.modules.Models import ModelInterface


@pytest.fixture
def model_interface():
    # Set up any necessary objects or state before each test
    return ModelInterface(
        dict(
            model_root="models",
            hf_model_repo="SurfaceAI/models",
            models={
                "surface_type": "v1/surface_type_v1.pt",
                "surface_quality": {
                    "asphalt": "v1/surface_quality_asphalt_v1.pt",
                    "concrete": "v1/surface_quality_concrete_v1.pt",
                    "paving_stones": "v1/surface_quality_paving_stones_v1.pt",
                    "sett": "v1/surface_quality_sett_v1.pt",
                    "unpaved": "v1/surface_quality_unpaved_v1.pt",
                },
                "road_type": "v1/road_type_v1.pt",
            },
            gpu_kernel=0,
            transform_surface={"resize": 384, "crop": "lower_middle_half"},
            transform_road_type={"resize": 384, "crop": "lower_half"},
            batch_size=48,
        )
    )


def test_model_predict(model_interface):
    input_data = []
    for image_id in ["1000068877331935", "458670231871080", "1000140361462393"]:
        image_path = os.path.join(root_dir, "tests", "test_data", f"{image_id}.jpg")
        input_data.append(Image.open(image_path))

    expected_output = [
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
        [
            "1_1_road__1_1_road_general",
            round(0.9890756607055664, 5),
            "asphalt",
            round(0.9999390840530396, 5),
            round(1.8645464181900024, 5),
        ],
    ]

    output = model_interface.batch_classifications(input_data)
    assert output == expected_output
