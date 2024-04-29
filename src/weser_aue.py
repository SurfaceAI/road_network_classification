import os
import pipeline as pl
import utils
data_path = "data"
### USER INPUT: define bounding box ###

minLon=8.922732223970124
minLat=52.55664084766004
maxLon=9.200080702809483
maxLat=52.751913856234566
name = "weser_aue"
pred_path = "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/training/prediction/weseraue/effnet_surface_quality_prediction-weseraue_imgs_2048-20240425_165002.csv"


data_path = os.path.join(data_path, name)

pl.img_sample(data_path, minLon, minLat, maxLon, maxLat, name, userid="109527895102408", no_pano=False) # weseraue2022
#pl.img_selection(data_path, minLon, minLat, maxLon, maxLat, name)
#pl.img_download(name)
#pl.img_download(data_path=data_path, db_table="weser_aue_point_selection")

# for all panorama images in the folder, create perspective images
# ct = 0
# for img_id in utils.img_ids_from_dbtable("weser_aue_point_selection"):
#     if ct % 100 == 0:
#         print(f"{ct} images of {len(utils.img_ids_from_dbtable('weser_aue_point_selection'))}")
#         ct += 1
#     utils.pano_to_perspective(img_id, os.path.join(data_path, "panorama_2048"), os.path.join(data_path, "imgs_2048"), angle=0.0)
    
#pl.img_classification(data_path, name, pred_path)
#pl.aggregate_by_road_segment(name)
