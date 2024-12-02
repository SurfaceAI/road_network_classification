import numpy as np
from equilib import equi2pers
from PIL import Image

# compute correct yaw given the the front of the camera
# TODO % (2 *np.pi) instead?
def compute_yaw(compass_angle, direction_of_travel):
    yaw = (compass_angle - direction_of_travel) / 180 * np.pi
    # adjust to 2pi
    if yaw < 0:
        return 2 * np.pi + yaw
    elif yaw > 2 * np.pi:
        return yaw - 2 * np.pi
    else:
        return yaw


def compute_direction_of_travel(neighbor_coords):
    x = [neighbor_coord[0] for neighbor_coord in neighbor_coords]
    y = [neighbor_coord[1] for neighbor_coord in neighbor_coords]
    # Get images before and after
    # Fit a linear regression line
    slope, _ = np.polyfit(x, y, 1)
    # Compute the angle of the line in radians
    angle = np.arctan(slope)
    # Convert the angle to angle in degrees
    degrees = np.degrees(angle)
    # Convert to compass angle
    dir_of_travel = 90 - degrees

    # mind the direction
    # if right to left, then + 180
    if x[0] > y[-1]:
        dir_of_travel += 180
    return dir_of_travel


def pano_to_persp(
    img,
    yaw,
    direction,
    persp_height=480,
    persp_width=640,
):
    """Transform panorama image to two perspective images that face the direction of travel
    Args:   img (?): image data
            compass_angle (float): computed camera angle (as given by mapillary). If None, cangle is queried from mapillary
            persp_height (int): height of target perspective image
            persp_width (int): width of target perspective image
    """
    # rotations
    rots = {
        "roll": 0.0,
        "pitch": 0,  # rotate vertical (look up and down)
        "yaw": yaw
        + (
            direction * np.pi
        ),  # rotate horizontal (look left and right) - np.pi = 180 degrees
    }

    equi_img = np.asarray(img)
    equi_img = np.transpose(equi_img, (2, 0, 1))

    # Run equi2pers
    pers_img = equi2pers(
        equi=equi_img,
        rots=rots,
        height=persp_height,  # height, width (int): perspective size
        width=persp_width,  # height, width (int): perspective size
        fov_x=90.0,  # perspective image fov of x-axis
        mode="bilinear",
    )

    # transpose back to image format
    pers_img = np.transpose(pers_img, (1, 2, 0))
    return Image.fromarray(pers_img)