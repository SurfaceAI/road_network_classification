
import os
import utils

if __name__ == "__main__":

    input_folder = "/Users/alexandra/Documents/GitHub/road_network_classification/data/other_pano"
    output_folder = "/Users/alexandra/Documents/GitHub/road_network_classification/data/other_pano_as_persp"
    
    file_names = os.listdir(input_folder)
    img_ids = [file_name.split(".")[0] for file_name in file_names]

    for img_id in img_ids:
        utils.pano_to_persp(input_folder, output_folder, img_id)





