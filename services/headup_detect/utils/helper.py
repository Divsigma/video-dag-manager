import math
import numpy as np

def get_roi(bounding_boxes, width, height):
    x_min = width
    y_min = height
    x_max = 0
    y_max = 0

    for x1, y1, x2, y2 in bounding_boxes:
        x_min = min(x_min, x1)
        y_min = min(y_min, y1)
        x_max = max(x_max, x2)
        y_max = max(y_max, y2)

    return x_min, y_min, x_max, y_max

def isRotationMatrix(R) :
    Rt = np.transpose(R)
    shouldBeIdentity = np.dot(Rt, R)
    I = np.identity(3, dtype = R.dtype)
    n = np.linalg.norm(I - shouldBeIdentity)
    return n < 1e-6

def normalize(theta):

    if theta > 90:
        ret = theta - 180
    elif theta < -90:
        ret = theta + 180
    else:
        ret = theta

    return ret

def rotation_matrix_to_euler_angle(R):

    # assert (isRotationMatrix(R))
    sy = math.sqrt(R[0, 0] * R[0, 0] + R[1, 0] * R[1, 0])
    singular = sy < 1e-6
    factor = 180 / math.pi

    if not singular:
        x = math.atan2(R[2, 1], R[2, 2])
        y = math.atan2(-R[2, 0], sy)
        z = math.atan2(R[1, 0], R[0, 0])
    else:
        x = math.atan2(-R[1, 2], R[1, 1])
        y = math.atan2(-R[2, 0], sy)
        z = 0

    x = normalize(x * factor)
    y = normalize(y * factor)
    z = normalize(z * factor)

    return x, y, z

    # return x * factor, y * factor, z * factor

def valid (value, v1, v2):
    return value >= v1 and value <= v2