create index if  not exists temp_classification_updates_idx on temp_classification_updates(img_id);
create index if not exists dresden_small_img_metadata_id_idx on dresden_small_img_metadata(img_id);

drop table if exists temp_join;
create table temp_join as
(SELECT img.*,
    res.road_type_pred,
    res.road_type_prob,
    res.type_pred,
    res.type_class_prob,
    res.quality_pred
FROM {name}_img_metadata img
LEFT JOIN temp_classification_updates res
ON img.img_id = res.img_id);

drop table  dresden_small_img_metadata;
ALTER TABLE temp_join RENAME TO {name}_img_metadata;

DROP TABLE temp_classification_updates;