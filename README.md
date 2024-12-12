# SurfaceAI: pipeline for surface type and quality classification of road networks

This repository provides the code for the SurfaceAI pipeline.

For a specified bounding box, a Shapfile is generated that contains the surface type and quality classifications on a road network. Therefore, images from Mapillary are downloaded, classified using CNN models and aggregated on a provided (OpenStreetMap) road network.


![Schematic illustration of model pipeline](img/model_pipeline.png)


Find the [paper]([https://arxiv.org/abs/2409.18922](https://dl.acm.org/doi/10.1145/3681780.3697277)) of this publication here.

Cite as:

```
    @inproceedings{10.1145/3681780.3697277,
        author = {Kapp, Alexandra and Hoffmann, Edith and Weigmann, Esther and Mihaljevi\'{c}, Helena},
        title = {SurfaceAI: Automated creation of cohesive road surface quality datasets based on open street-level imagery},
        year = {2024},
        publisher = {Association for Computing Machinery},
        address = {New York, NY, USA},
        doi = {10.1145/3681780.3697277},
        booktitle = {Proceedings of the 2nd ACM SIGSPATIAL International Workshop on Advances in Urban-AI},
        pages = {54–57},
        numpages = {4},
        location = {Atlanta, GA, USA},
        series = {UrbanAI '24}
    }
```

## Getting started

### Prerequisites

-  A Postgis database is used for faster geocomputations. This requires prior installation of `postgresql`, `postgis`, `osmosis` (E.g., with `brew install` for MacOS and `apt install` for Linux)

- Create a `02_credentials.json` file according to `02_credentials_example.json`.
You need to provide a database user name and password with which you may access your Postgres databases (this may need to be configured on your server). You need to ensure that your database user has `superuser`rights to create extensions (postgis and hstore).
- A Mapillary access token is required and needs to be provided in the `02_credentials.json` file. You can obtain a free token as described [here](https://help.mapillary.com/hc/en-us/articles/360010234680-Accessing-imagery-and-data-through-the-Mapillary-API#h_e18c3f92-8b3c-4d26-8a1b-a880bde3a645).


### Setup Python environment

Using [`poetry`](https://python-poetry.org/) for dependency management, install poetry: 


```bash 
    pip install pipx
    pipx install poetry
```

Create an environment using poetry

```bash 
    poetry shell
```

Install required packages, as defined in `pyproject.toml`

```bash 
    poetry install
```

### Quick Start (TL;DR)

Use the `configs/01_1_area_of_interest_config_example.json` as a template for `my_config_file.json` and specify attributes `name` (str), `minLon`, `minLat`, `maxLon`, `maxLat` of the bounding box of your area of interest.
Limit the OSM road network (global config parameter `osm_region`) to the required scope ("germany" takes approx. 30GB database storage).

If environment is not active, activate with

```bash 
    poetry shell
```

Execute the pipeline with 


```bash
    python src/main.py -c my_config_file
```

The created dataset is stored in `data/output/<NAME_FROM_CONFIG>_surfaceai.shp`

If database to create further area of interest datasets is no longer needed, remove database with (OSM) road network:

```bash
    dropdb YOUR_DBNAME
```

## Advanced configuration options


**Configuration files**

The configuration files are constructed to provide one global configuration that sets parameters regardless of the specific *area of interest*. For each area of interest, defined by its geographical bounding box, a dedicated config file is used. 
This allows you to specify multiple areas of interest. Within the specific configuration file, you may overwrite any global parameter. 
The `00_global_config.json` is always considered, while you provide the area of interest config file name when starting the program (without `.json` file ending): `python src/main.py -c my_config_file` 

- Specify region for the underlying OSM road network with `osm_region` suitable for your area(s) of interest. Names as available from Geofabrik (e.g., "germany", "berlin", "hessen"). E.g., you can specify "germany" if you have mulitple municipalities all over Germany as areas of interest. If you are only interested in a certain region, specify a smaller region, as the initialization runs faster and requires less storage.
- If you already have pbf files downloaded and stored at a different location, you can change the `pbf_folder`in the global config. Otherwise, the file will be downloaded automatically from Geofabrik to `data/osm_pbf`
- Specify the the bounding box (`minLon`, `minLat`, `maxLon`, `maxLat`) of the area of interest in `configs/my_config_file.json` file and provide a `name`. See the example config file `configs/01_1_area_of_interest_config_example.json`.
- If you want to use a different road network than OSM, set the parameter `osm_region=None` and set `road_network_path` to your Shapefile source file location (either within the global config, if this is true for all area of interests, or only within the specific config file, if this is only true for a single area of interest. In the latter case, you also need to provide a new database name `dbname`, as the new road network requires a new database). 
The custom road network dataset is expected to have a `geom` column and a `road_type` column (with values from: `road`, `path`, `sidewalk`, `cycleway`, `bike_lane`). 
If no road type value is available, a column with `null` values will be initialized automatically. For `null` values all potential road types will be returned (i.e., every road will have an additional cycleway and sidewalk geometry). 
You may have an identifier column in your custom road network that you wish to maintain in the output. This can be specified via the config parameter `additional_id_column`.
See the example `configs/01_2_custom_road_network_area_of_interest_config_example.json`

- In `00_global_config.json` global config parameters may be adjusted (however, reasonable defaults are set)
They consist of the following: 
    - Database name `dbname`
    - Mapillary API specifications: 
        - `img_size`indicates the size of the Mapillary image to download, given by the image width. Options according to the Mapillary API: `thumb_original_url`, `thumb_2048_url`, `thumb_1024_url`, `thumb_256_url`
        - `parrallel`(bool), whether image download should be parallelized
        - `parallel_batch_size`maximum images to download in parallel
    - Geospatial operation parameters:    
        - `proj_crs`: EPSG code of projected CRS to use for distance computations (for areas of interest in Europe 3035 is suitable)
        - `dist_from_road`: maximum distance from road in CRS unit (usually meters) for an image to be assigned to the road,
        - `segment_length`: length of a subsegment for aggregation algorithm,
        - `min_road_length`: short roads, which are common in OSM, like driveways, can be excluded to reduce cluttering and noise. This parameter specifies the minimum road length to be included.
        - `segments_per_group`: The length of a classified road segement in number of subsegments. E.g., if `segment_length` = 20 meters and `segments_per_group` = 3, then the output will result in 3x20=60 meter road segments. If set to `null`, then the road segments are maintained as given in the input of the road network.
    - Classification model specific parameters:
        - `model_root`: path to root folder of models
        - `models`(dict): required keys: `road_type`, `surface_type`, `surface_quality`(with sub keys `asphalt`, `concrete`, `paving_stones`, `sett`, `unpaved`). Each value indicates the pt. file locaiton of the respective model weights 
        - `gpu_kernel`: if more than one GPU kernel is available, the one to be used can be specified here
        - `transform_surface` and `transform_road_type` (dict): with keys `resize`and `crop`specifying the transform operations conducted on images for these models
        - `batch_size`model batch size

You can overwrite any parameter in the specific config. E.g., the `dist_to_road`shall be 10 meters for all area of interest, except one, then you can set `dist_to_road`=10 within the global config and overwrite the parameter for only a single area of interst by setting `dist_to_road`=20 within the specific config.


**Arguments for command-line options for main.py**

```
    usage: surfaceAI [-h] [-c CONFIGFILE] [--recreate_roads | --no-recreate_roads] [--query_images | --no-query_images] [--export_results | --no-export_results] [--export_img_predictions | --no-export_img_predictions]

    optional arguments:
    -h, --help            show this help message and exit
    -c CONFIGFILE, --configfile CONFIGFILE
                            Name of the configuration file in the configs folder. Required argument.
    --recreate_roads, --no-recreate_roads
                            If False, omit preprocessing or road segments if already present in database (to save time given multiple runs on the same area of interest). (default: False)
    --query_images, --no-query_images
                            If False, skip classification of newly queried images and only use existing image classifications in database. (default: True)
    --export_results, --no-export_results
                            Export results to Shapefile (default: True)
    --export_img_predictions, --no-export_img_predictions
                            Export single image predictions to Shapefile (default: False)
```


## Output

### Road network classification

The created surfaceai output shapefile includes the following attributes:

- **ID**: (OSM) way ID
- **PART_ID**: partition ID - if multiple road types are present for a single OSM geometry (e.g.: OSM tag `sidewalk = right` present), the road is duplicated and shifted, according to the road type. Additionally, the `road_type`attribute is adjusted. The following partition IDs are set: 
    - `1`: `default` (according to `highway` tag of a geometry)
    - `2`: `sidewalk = 'right'` -> footway
    - `3`: `sidewalk = 'left'` -> footway
    - `4`: `cycleway_right = 'lane'` -> bike_lane
    - `5`: `cycleway_left = 'lane'` -> bike_lane
    - `6`: `cycleway_right = 'track'`-> cycleway
    - `7`: `cycleway_left = 'track'`-> cycleway
- **GROUP_NUM**: enumeration of divisions of the original geometry based on `segments_per_group`(see above)
- **TYPE_PRED**: surface type prediction (see below)
- **CONF_SCORE**: confidence score of surface type (*share of subsegments consistent with predicted type multiplied by share of images consistent with predicted type*)
- **QUALI_PRED**: surface quality prediction (see below)
- **N_IMGS**: number of images considered for the respective road segment
- **MIN_DATE**: earliest capture date of considered images for respective road segment
- **MAX_DATE**: most recent capture date of considered images for respective road segment
- **ROAD_TYPE**: road type (options: ["road", "footway", "bike_lane", "cycleway", "path"]) based on OSM data simplified to the stated options 
- **GEOM**: Linestring geometry

### Single image classification

If results of single image classification are exported, the respective shapefile contains the following attributes:

- **IMG_ID**: Mapillary image id
- **DATE**: capture date of image according to Mapillary
- **ROADT_PRED**: road type prediction according to the classification model, simplified into the following options: ["road", "footway", "bike_lane", "cycleway", "path"]
- **ROADT_PROB**: class probability of the predicted road type class according to the classification model
- **TYPE_PRED**: surface type prediction according to the classification model
- **TYPE_PROB**: class probability of the predicted surface type class according to the classification model
- **QUALI_PRED**: quality prediction according to the regression model


## Implementation details

### Pipeline:

- setup Postgres database with PostGIS and osmosis extension
- query all image metadata within the provided bounding box from Mapillary and write to database
- create subsegments of road segments within the bounding box (based on OSM if no other network is provided)
- match images to closest road distance 
- download and classify all relevant images from Mapillary (depending on the number of images, this step may take a while)
- aggregate single classifications to road network classification (see details below)
- store Shapefile of results

### Surface classification 

See https://github.com/SurfaceAI/classification_models

### Aggregation algorithm 

The aggregation algorithm runs as follows:

- aggregate images on subsegments; only use images that are either not within the vicinity of another road (part) or where the road scene classification matches the road type of the segment
    - surface type: majority vote
    - surface quality: average
- aggregate all subsegments on road segment (or group, if `segments_per_group` is given)
    - surface type: majority vote (if tied, also use image counts)
    - surface quality: average

Set predition to `null` if 
- remove predictions where there is no prediction for respective road type every 30 meters 
- or less than 50% of segments are of predicted type 
- or if there is less than 3 images









