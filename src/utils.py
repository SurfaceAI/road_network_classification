import csv


def img_ids_from_csv(csv_path, img_id_col=1):
    with open(csv_path, newline="") as csvfile:
        csvreader = csv.reader(csvfile)
        image_ids = [row[img_id_col] for row in csvreader][1:]
    return image_ids


def clean_surface(surface):
    if surface in [
        "compacted",
        "gravel",
        "ground",
        "fine_gravel",
        "dirt",
        "grass",
        "earth",
        "sand",
    ]:
        return "unpaved"
    elif surface in ["cobblestone", "unhewn_cobblestone"]:
        return "sett"
    elif surface in ["concrete:plates", "concrete:lanes"]:
        return "concrete"
    elif surface in ["grass_paver"]:
        return "paving_stones"
    else:
        return surface


def format_predictions(model_prediction, is_pano):
    """Bring model prediction output into a format for further analysis

    Args:
        model_prediction (pd.DataFrame): model prediction csv output
        is_pano (bool, optional): are images panorama images with `Image`indication direction (_0 and _1)?

    Returns:
        pd.DataFrame: formatted model predictions
    """

    type_prediction = model_prediction.loc[model_prediction["Level"] == "type"].copy()
    # the prediction holds a value for each surface and a class probability. Only keep the highest prob.
    idx = type_prediction.groupby("Image")["Prediction"].idxmax()
    type_prediction = type_prediction.loc[idx]
    type_prediction.rename(
        columns={"Prediction": "type_class_prob", "Level_0": "type_pred"}, inplace=True
    )

    quality_prediction = model_prediction.loc[
        model_prediction["Level"] == "quality"
    ].copy()
    quality_prediction.rename(
        columns={"Prediction": "quality_pred", "Level_1": "quality_pred_label"},
        inplace=True,
    )

    pred = type_prediction.set_index("Image").join(
        quality_prediction.set_index("Image"), lsuffix="_type", rsuffix="_quality"
    )
    pred = pred[["type_pred", "type_class_prob", "quality_pred", "quality_pred_label"]]
    pred.reset_index(inplace=True)

    if is_pano:
        img_ids = pred["Image"].str.split("_").str[0:2]
        pred.insert(0, "img_id", [img_id[0] for img_id in img_ids])
        pred.insert(1, "direction", [int(float(img_id[1])) for img_id in img_ids])
        pred.drop(columns=["Image"], inplace=True)
    if not is_pano:
        pred.rename(columns={"Image": "img_id"}, inplace=True)
    return pred


def format_scenery_predictions(model_prediction, is_pano):
    """Bring model scenery prediction output into a format for further analysis

    Args:
        model_prediction (pd.DataFrame): model prediction csv output
        pano (bool, optional): are images panorama images with `Image`indication direction (_0 and _1)?.

    Returns:
        pd.DataFrame: formatted model predictions
    """

    # the prediction holds a value for each surface and a class probability. Only keep the highest prob.
    idx = model_prediction.groupby("Image")["Prediction"].idxmax()
    model_prediction = model_prediction.loc[idx]
    model_prediction.rename(
        columns={"Prediction": "type_class_prob", "Level_0": "scenery_pred"},
        inplace=True,
    )

    model_prediction = model_prediction[["Image", "scenery_pred"]]

    if is_pano:
        img_ids = model_prediction["Image"].str.split("_").str[0:2]
        model_prediction.insert(0, "img_id", [img_id[0] for img_id in img_ids])
        model_prediction.insert(
            1, "direction", [int(float(img_id[1])) for img_id in img_ids]
        )
        model_prediction.drop(columns=["Image"], inplace=True)
    if not is_pano:
        model_prediction.rename(columns={"Image": "img_id"}, inplace=True)
    return model_prediction
