
import utils

if __name__ == "__main__":
    # Input equirectangular image

    # TODO: compute direction of travel
    # get 2 points before, 2 after and approx. line

    
    img_id = "995820478296286"
    cangle = 336.74
    #cangle = 114.69
    # direction_of_travel = 120

    img_id = "678698493434590"
    cangle = 211.28
    direction_of_travel = 160

    img_id = "881163283390519"
    cangle = 21.716

    img_id = "881163283390519"
    cangle = 21.716

    img_id = "881163283390519"
    cangle = 21.716


    img_id = "881163283390519"
    cangle = 21.716

    img_id = "234474518929231"
    cangle = 260.35

    in_path = "/Users/alexandra/Documents/GitHub/road_network_classification/data/panorama"
    out_path = "data/panorama/persp/"

    utils.pano_to_persp(in_path, out_path, img_id, cangle)


    #img_id = "881163283390519"
    #cangle = 21.716


    #img_id = "100555886002173"
    #cangle = 36.51

    #img_id = "970653480472788"
    #cangle = 343.80
    #cangle = 0

    #img_id = "4445842562183315"
    #cangle = 156.85
    #direction_of_travel = 100





