import flask
import flask_cors
import threading
import json
import time
import cv2
import queue
import numpy as np

video_app = flask.Flask(__name__)
flask_cors.CORS(video_app)

info = dict()
income_q = None
res_jpg_byte_q = dict()
current_frame = None

def update_info():
    with video_app.app_context():
        global info
        while True:
            yield json.dumps(info).encode()
            # time.sleep(1)

@video_app.route('/')
def show_info():
    #return flask.Response(update_info(), mimetype='text/plain')
    return flask.render_template('index.html', info=info)

# video_cap = cv2.VideoCapture('input/video/input.mov')
# video_cap = cv2.VideoCapture('1')
def get_video_frame_result(jpeg_bytes_q):
    while True:
        frame_bytes = jpeg_bytes_q.get()
        # ret, frame = video_cap.read()

        if frame_bytes is not None:

            # ret, jpeg = cv2.imencode('.jpg', frame)
            # frame = jpeg.tobytes()
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + frame_bytes + b'\r\n\r\n')
        else:
            yield(b'--frame\r\n'
                  b'Content-Type: image/jpeg\r\n\r\n' + b'' + b'\r\n\r\n')

@video_app.route('/user/video/<job_uid>')
@flask_cors.cross_origin()
def video_feed(job_uid):
    global res_jpg_byte_q

    if job_uid not in res_jpg_byte_q:
        return flask.jsonify({"status": 1, "msg": "video not ready"})
    
    return flask.Response(get_video_frame_result(res_jpg_byte_q[job_uid]),
                          mimetype='multipart/x-mixed-replace; boundary=frame')
    # return flask.Response(get_video_frame_result_from_resq(),
    #                       mimetype='multipart/x-mixed-replace; boundary=frame')


from logging_utils import root_logger

def dispatch():
    global income_q, res_jpg_byte_q

    while True:
        root_logger.info("waiting income")
        res = income_q.get()
        job_uid = res["job_uid"]
        if "image_type" in res:
            image_type = res["image_type"]
            assert image_type == "jpeg"
        image_bytes = res["image_bytes"]
        root_logger.info("got image from job-{}".format(job_uid))

        if job_uid not in res_jpg_byte_q:
            res_jpg_byte_q[job_uid] = queue.Queue(20)

        assert isinstance(res_jpg_byte_q[job_uid], queue.Queue)

        if res_jpg_byte_q[job_uid].full():
            res_jpg_byte_q[job_uid].get()
        res_jpg_byte_q[job_uid].put(image_bytes)
        root_logger.info("res_q[{}] size={}".format(job_uid, res_jpg_byte_q[job_uid].qsize()))

def init_and_start_video_proc(q, serv_port=5100):
    root_logger.info("start init_and_start_video_proc")

    global income_q, video_app
    income_q = q

    dispatch_thread = threading.Thread(target=dispatch, daemon=True)
    dispatch_thread.start()

    video_app.run('0.0.0.0', port=serv_port)

class FlaskWebThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
    
    def run(self):
        app.run('0.0.0.0')

if __name__ == '__main__':
    FlaskWebThread().start()
