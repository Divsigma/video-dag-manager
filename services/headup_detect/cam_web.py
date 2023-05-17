import flask
import threading
import json
import time
import cv2
import queue
import numpy as np
from utils import utils

app = flask.Flask(__name__)

info = dict()
frame_q = None
res_q = None
current_frame = None

def update_info():
    with app.app_context():
        global info
        while True:
            yield json.dumps(info).encode()
            # time.sleep(1)

@app.route('/')
def show_info():
    #return flask.Response(update_info(), mimetype='text/plain')
    return flask.render_template('index.html', info=info)

# video_cap = cv2.VideoCapture('input/video/input.mov')
# video_cap = cv2.VideoCapture('1')
def get_video_frame_result():
    while True:
        frame = frame_q.get()
        # ret, frame = video_cap.read()
        if frame is not None:
            ret, jpeg = cv2.imencode('.jpg', frame)
            frame = jpeg.tobytes()
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n\r\n')
        else:
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + b'' + b'\r\n\r\n')

def get_video_frame_result_from_resq():
    while True:
        res = res_q.get()
        image = None

        if res['task_name'] == 'C':
            image = res['input_ctx']['image']
            bbox = res['input_ctx']['bbox']

            # render bbox
            bbox = np.reshape(bbox, (-1, 4)).astype(int)
            for i, box in enumerate(bbox):
                cv2.rectangle(image, (box[0], box[1]), (box[2], box[3]),
                              (0, 255, 0), 4)

        elif res['task_name'] == 'R':
            image = res['input_ctx']['image']
            bbox = res['input_ctx']['bbox']
            head_pose = res['input_ctx']['head_pose']
    
            axis, up, total, thres = [], 0, 0, -10 

            # render bbox
            bbox = np.reshape(bbox, (-1, 4)).astype(int)
            for i, box in enumerate(bbox):
                total += 1
                cv2.rectangle(image, (box[0], box[1]), (box[2], box[3]),
                              (255, 0, 0), 4)

            # render head pose
            for yaw, pitch, roll, tdx, tdy, size in head_pose:
                if pitch > thres:
                    up += 1
                ax = utils.draw_axis(image, yaw, pitch, roll, tdx=tdx, tdy=tdy, size=size)
                axis.append(ax)
            for ax in axis:
                ax = [int(x) for x in ax] 
                cv2.line(image, (ax[0], ax[1]), (ax[2], ax[3]), (0, 0, 255), 3)
                cv2.line(image, (ax[0], ax[1]), (ax[4], ax[5]), (0, 255, 0), 3)
                cv2.line(image, (ax[0], ax[1]), (ax[6], ax[7]), (0, 255, 255), 2)

            # render stats
            cv2.putText(image, 'Up: {} Total: {}'.format(up, total), (50, 50),
                        cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)

        else:
            print('[{}] [WARNING] unsupported task_name in res'.format(__name__))

        if image is not None:
            ret, img_jpeg = cv2.imencode('.jpg', image)
            img_bytes = img_jpeg.tobytes()
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + img_bytes + b'\r\n\r\n')
        else:
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + b'' + b'\r\n\r\n')

@app.route('/video')
def video_feed():
    # return flask.Response(get_video_frame_result(),
    #                       mimetype='multipart/x-mixed-replace; boundary=frame')
    return flask.Response(get_video_frame_result_from_resq(),
                          mimetype='multipart/x-mixed-replace; boundary=frame')

def init_and_start_ui_proc(res_queue):
    global res_q, app
    res_q = res_queue
    app.run('0.0.0.0')

class FlaskWebThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        app.run('0.0.0.0')

if __name__ == '__main__':
    FlaskWebThread().start()
