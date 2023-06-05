import cv2
import numpy as np
import math

resolution_wh = {
    "360p": {
        "w": 480,
        "h": 360
    },
    "480p": {
        "w": 640,
        "h": 480
    },
    "720p": {
        "w": 1280,
        "h": 720
    },
    "1080p": {
        "w": 1920,
        "h": 1080
    }
}

def resolution_test():
    cap = cv2.VideoCapture("input/input1.mp4")

    ret, frame = cap.read()

    print("{}".format(np.shape(frame)))

    resol = "360p"

    cap.set(cv2.CAP_PROP_FRAME_WIDTH, resolution_wh[resol]["w"])
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, resolution_wh[resol]["h"])

    ret, frame = cap.read()

    frame = cv2.resize(frame, [resolution_wh[resol]["w"], resolution_wh[resol]["h"]])

    print("{}, cap w: {}, cap h: {}".format(np.shape(frame),
                                            cap.get(cv2.CAP_PROP_FRAME_WIDTH),
                                            cap.get(cv2.CAP_PROP_FRAME_HEIGHT)))

def get_next_conf_frame(cap, conf_fps, video_fps, curr_conf_frame_id):
    frame = None
    new_video_frame_id = None
    new_conf_frame_id = None
    while True:
        # 从video_fps中实际读取
        video_frame_id = cap.get(cv2.CAP_PROP_POS_FRAMES)
        ret, frame = cap.read()
        assert ret

        conf_frame_id = math.floor((conf_fps * 1.0 / video_fps) * video_frame_id)
        if conf_frame_id > curr_conf_frame_id:
            new_video_frame_id = video_frame_id
            new_conf_frame_id = conf_frame_id
            break
    
    return new_video_frame_id, new_conf_frame_id, frame

def fps_test():
    cap = cv2.VideoCapture("input/input1.mp4")

    video_fps = cap.get(cv2.CAP_PROP_FPS)
    conf_fps = 24

    curr_video_frame_id = 0
    curr_conf_frame_id = 0

    while True:
        # 实现conf_fps
        frame = None
        video_frame_id, conf_frame_id, frame = get_next_conf_frame(cap, conf_fps, video_fps, curr_conf_frame_id)
        print("video_frame_id={} conf_frame_id={}".format(video_frame_id, conf_frame_id))

        curr_video_frame_id = video_frame_id
        curr_conf_frame_id = conf_frame_id

if __name__ == '__main__':
    # resolution_test()

    fps_test()
