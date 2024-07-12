import os
import pipeline as pl
import utils
data_path = "data"
### USER INPUT: define bounding box ###

minLon=8.92273
minLat=52.5566
maxLon=9.23894
maxLat=52.7519

name = "weser_aue"
pred_path = "/Users/alexandra/Nextcloud-HTW/SHARED/SurfaceAI/data/mapillary_images/training/prediction/weseraue/effnet_surface_quality_prediction-weseraue_imgs_2048-20240429_071045.csv"


data_path = os.path.join(data_path, name)

#pl.img_sample(data_path, minLon, minLat, maxLon, maxLat, name, userid="109527895102408", no_pano=False) # weseraue2022
#pl.img_selection(data_path, minLon, minLat, maxLon, maxLat, name)
#pl.img_download(data_path, dest_folder_name="panorama_2048", db_table=f"{name}_point_selection")

# for all panorama images in the folder, create perspective images
# ct = 0
# for img_id in utils.img_ids_from_dbtable(f"{name}_point_selection"):
    
#     if ct % 100 == 0:
#         print(f"{ct} images of {len(utils.img_ids_from_dbtable(f'{name}_point_selection'))}")
#     ct += 1


#     # skip images that are already transformed
#     if os.path.exists(os.path.join(data_path, "imgs_2048", f"{img_id}_0.jpg")):
#         continue
#     # skip images that do not exist
#     if not os.path.exists(os.path.join(data_path, "panorama_2048", f"{img_id}.jpg")):
#         continue
#     utils.pano_to_persp(os.path.join(data_path, "panorama_2048"), 
#                         os.path.join(data_path, "imgs_2048"), 
#                         img_id, cangle=0, direction_of_travel=0) # we dont need to compute direction of travel 
#pl.img_classification(data_path, name, pred_path)
pl.aggregate_by_road_segment(name)
