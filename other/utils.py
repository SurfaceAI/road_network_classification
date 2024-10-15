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

