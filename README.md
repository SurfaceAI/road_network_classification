# SurfaceAI: pipeline for surface type and quality classification of road networks

This repository provides the code for the SurfaceAI pipeline. 

## Prerequisites

- To setup the database, requires prior installation of postgresql, postgis, osmosis.
- Download of road network. If OSM is used, as .pbf file. Speficy pbf file location in `config.py`.

## User Input

- Specify the the bounding box of the region of interest in the `config.py` file.
- If you want to use a different road network than OSM, adjust the parameter `TODO`
- If you want to use different images than Mapillary, adjust the parameter `TODO`
- State the output file name `TODO`

## Run SurfaceAI

Start the pipeline by running


```python
    python src/pipeline.py  config_name
```





