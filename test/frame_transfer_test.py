import requests
import time
import cv2
import multiprocessing as mp
import field_codec_utils
import pickle
import json

def producer(q):
    cap = cv2.VideoCapture("input.mov")
    ret, frame = cap.read()
    frame = field_codec_utils.encode_image(frame)
    t = time.time()
    req = {
        "time": t,
        "frame": frame
    }
    q.put(req)

def consumer(q):
    req = q.get()
    t = time.time()
    print("delta_t_pipe={}".format(t - req['time']))

def http_delay_test():
    sess = requests.Session()

    cap = cv2.VideoCapture("input.mov")
    ret, frame = cap.read()
    frame = field_codec_utils.encode_image(frame)

    st_time = time.time()

    r = sess.post("http://127.0.0.1:5500/execute_task/face_detection",
            json={"image": frame})
            # json={"image": json.dumps(frame.tolist())})
            # json={"image": str(pickle.dumps(frame.tolist(), 0))})

    ed_time = time.time()

    print("delay_t_http={}".format(ed_time - st_time))

if __name__ == "__main__":
    q = mp.Queue(1)
    pp = mp.Process(target=producer, args=(q,))
    pc = mp.Process(target=consumer, args=(q,))

    pc.start()
    pp.start()

    pc.join()
    pp.join()

    http_delay_test()
