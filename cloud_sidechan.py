import flask
import flask_cors
import threading
import json
import time
import cv2
import queue
import numpy as np

import requests

from logging_utils import root_logger

video_app = flask.Flask(__name__)
flask_cors.CORS(video_app)

serv_port = 5100
node_addr = dict()

@video_app.route('/user/video/<job_uid>')
@flask_cors.cross_origin()
def video_feed(job_uid):
    global serv_port, node_addr

    if job_uid not in node_addr:
        return flask.jsonify({"status": 1, "msg": "video not ready on cloud"})

    # 流式请求转发
    req = requests.get("http://{}/user/video/{}".format(node_addr[job_uid], job_uid),
                       stream=True)
    
    return flask.Response(flask.stream_with_context(req.iter_content()),
                          content_type=req.headers['content-type'])

@video_app.route('/user/update_node_addr', methods=["POST"])
@flask_cors.cross_origin()
def node_update_node_addr_cbk():
    global node_addr

    para = flask.request.json
    job_uid = para['job_uid']
    addr = para['node_addr']
    node_addr[job_uid] = addr

    return flask.jsonify({"msg": "update <job_uid, addr> (<{}, {}>) to cloud".format(job_uid, addr)})


def init_and_start_video_proc(port=5100):
    root_logger.info("start init_and_start_video_proc")

    global serv_port
    serv_port = port

    video_app.run('0.0.0.0', port=port)

if __name__ == '__main__':
    pass
