import os
import sys
from pathlib import Path
import json
import logging 

#sys.path.append(root_path)

## local modules
from modules import SurfaceDatabase as sd, MapillaryInterface as mi, AreaOfInterest as aoi

if __name__ == "__main__":

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    root_path = str(Path(os.path.abspath(__file__)).parent.parent)
    global_config_path = os.path.join(root_path, "configs", "00_global_config.json")
    credentials_path = os.path.join(root_path, "configs", "01_credentials.json")
    if len(sys.argv) > 1:
        config_path = os.path.join(root_path, "configs", sys.argv[1])
    else:
        config_path = os.path.join(root_path, "configs", "dresden_small.json")


    # Read and parse the JSON configuration file
    with open(global_config_path, 'r') as config_file:
        global_cg = json.load(config_file)
    with open(config_path, 'r') as config_file:
        cg = json.load(config_file)
    cg = {**global_cg, **cg}
    with open(credentials_path, 'r') as cred_file:
        credentials = json.load(cred_file)
        
    mapillary_interface = mi.MapillaryInterface(
                                                mapillary_token = credentials["mapillary_token"], 
                                                parallel = cg["parallel"], 
                                                parallel_batch_size=cg["parallel_batch_size"])  

    # TODO: only append root_path if data_root is a relative path
    cg["data_root"] = os.path.join(root_path, cg["data_root"])
    area_of_interest = aoi.AreaOfInterest(cg)
    
    # TODO: only append root_path if pbf_path is a relative path
    cg["pbf_path"] = os.path.join(root_path, cg["pbf_path"])
    surface_database = sd.SurfaceDatabase(
                                        credentials[cg["dbname"]],
                                        credentials[cg["dbuser"]],
                                        credentials[cg["dbhost"]],
                                        credentials[cg["dbpassword"]],
                                        cg["pbf_path"])
    
    surface_database.process_area_of_interest(area_of_interest, mapillary_interface)

