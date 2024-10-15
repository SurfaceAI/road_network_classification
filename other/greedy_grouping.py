import geopandas as gpd
import pandas as pd

run = 8

df = gpd.read_file(
    f"/Users/alexandra/Documents/GitHub/road_network_classification/data/berlin_prio_vset/run{run}/berlin_segments_pred.shp",
    dtype={"ID": int},
    crs=25833,
)
df.rename(columns={"GROUP_NUM": "segment_number"}, inplace=True)
df = df[["ID", "segment_number", "ROAD_TYPE", "TYPE_PRED", "CONF_SCORE"]]

df["group_num"] = None
# iterate over ways
for way_id in df.ID.unique():
    # for way_id in [1713.0, 1253.0]:
    # get overall max segment number for this way (not depending on road type)
    seg_max = df.loc[df.ID == way_id, "segment_number"].max()

    # group for each road type seperately
    for road_type in df.loc[df.ID == way_id, "ROAD_TYPE"].unique():
        current_group = 0
        # div_count = 0 # how many segments diverge from the current type?
        types_in_group = []

        for i in range(0, seg_max + 1):
            current_segment = i

            # is there a value for this road type? else create a row
            if (
                len(
                    df[
                        (df.ID == way_id)
                        & (df.segment_number == current_segment)
                        & (df.ROAD_TYPE == road_type)
                    ]
                )
                == 0
            ):
                df = pd.concat(
                    [
                        df,
                        pd.DataFrame(
                            [
                                {
                                    "ID": way_id,
                                    "segment_number": current_segment,
                                    "ROAD_TYPE": road_type,
                                    "TYPE_PRED": None,
                                    "CONF_SCORE": -1,
                                }
                            ]
                        ),
                    ]
                )

            df.loc[
                (df.ID == way_id)
                & (df.segment_number == current_segment)
                & (df.ROAD_TYPE == road_type),
                "group_num",
            ] = current_group
            seg = df[
                (df.ID == way_id)
                & (df.segment_number == current_segment)
                & (df.ROAD_TYPE == road_type)
            ]

            current_type = (
                seg["TYPE_PRED"].values[0] if len(seg) == 1 else "undefined"
            )  # if more than one surface type pred (conf <= 0.5) then set to 'undefined'
            current_conf = seg["CONF_SCORE"].values[0] if len(seg) == 1 else -1

            if i < seg_max:
                next_segment = current_segment + 1
                next_seg = df[
                    (df.ID == way_id)
                    & (df.segment_number == next_segment)
                    & (df.ROAD_TYPE == road_type)
                ]
                next_type = (
                    next_seg["TYPE_PRED"].values[0] if len(next_seg) == 1 else None
                )
                # if more than one or no surface type is predicted set type to None

                # change group if two consecutive segments have the same type that differs from the current dominant type in the group
                dominant_type = (
                    max(types_in_group, key=types_in_group.count)
                    if len(types_in_group) > 0
                    else None
                )
                if (
                    (len(types_in_group) > 0)
                    and (current_type == next_type)
                    and (current_type != "undefined")
                    and (  # if no clear prediction, unify with current group
                        current_type != dominant_type
                    )
                    and (  # TODO: what if more than one dominant type?
                        len([i for i in types_in_group if i == dominant_type]) > 1
                    )  # dominant type needs to have appeared at least twice
                ):
                    current_group += 1
                    types_in_group = []
                    # change group number of current group retroactively (as this has also already diverged)
                    df.loc[
                        (df.ID == way_id)
                        & (df.segment_number == current_segment)
                        & (df.ROAD_TYPE == road_type),
                        "group_num",
                    ] = current_group
                elif (
                    (current_type is not None)
                    & (current_type != "undefined")
                    & (current_conf > 0.5)
                ):
                    types_in_group.append(current_type)


df["ID"] = df.ID.astype(int)
df.sort_values(by=["ID", "ROAD_TYPE", "segment_number"], inplace=True)
df[["ID", "ROAD_TYPE", "segment_number", "group_num"]].to_csv(
    f"/Users/alexandra/Documents/GitHub/road_network_classification/data/berlin_prio_vset/run{run}/berlin_segments_pred_grouped.csv",
    index=False,
)
