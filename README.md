# SurfaceAI: pipeline for surface type and quality classification of road networks

This repository provides the code for the SurfaceAI pipeline. 

! THIS CODE IS STILL UNDER DEVELOPMENT! 

*Currently, the classification models are not yet integrated and a csv file path `pred_path` to surface type and quality image classification model results is expected in the config file, as well as a `road_type_pred_path` to the road scene classification model results.*

Find the [paper](TODO) of this publication here.

## Getting started

### Prerequisites

- To setup the database, requires prior installation of `postgresql`, `postgis`, `osmosis`.
- Download of road network. If OSM is used, as `.pbf` file. Speficy pbf file location in `config.py`.

### User Input

- Specify the the bounding box of the region of interest in the `config.py` file and provide a `name`
- If you want to use a different road network than OSM, add a table with LINESTRINGs to your PostGis database, within your config, set the parameter `pbf_path=None` and adjust the parameter `"custom_attrs":{"edge_table_name": "SQL_TABLE_NAME"},`

### Run SurfaceAI

Create an environment and install requirements

```bash
    conda create --name surfaceai
    conda activate surfacai
    pip install -r requirements.txt
```

Start the pipeline by running


```python
    python src/pipeline.py  CONFIG_NAME
```

## Implementation details

### Pipeline:

![Schematic illustration of model pipeline](img/model_pipeline.png)

- setup Postgres database with PostGIS and osmosis extension
- query all image metadata within the provided bounding box from Mapillary and write to database
- create 20m subsegments of road segments within the bounding box (based on OSM if no other network is provided)
- match images to closest road, max. 10m distance (may be adjusted in the `config`)
- download all relevant images from Mapillary (depending on the number of images, this step may take a while)
- classify all road scene, surface type and quality of images (currently, this step is not yet integrated in this pipeline)
- aggregate single classifications to road network classification (see details below)
- store Shapefile of results

## Surface classification 

See https://github.com/SurfaceAI/classification_models

## Aggregation algorithm 

The aggregation algorithm runs as follows:

- aggregate images on 20m subsegments; only use images that are either not within the vicinity of another road (part) or where the road scene classification matches the road type of the segment
    - surface type: majority vote
    - surface quality: average
- aggregate all subsegments on road segment
    - surface type: majority vote (if tied, also use image counts)
    - surface quality: average
- futher attributes that are created per road segment:
    - min. and max. capture date of considered images
    - image count
    - confidence score of surface type (*share of subsegments consistent with predicted type multiplied by share of images consistent with predicted type*)







