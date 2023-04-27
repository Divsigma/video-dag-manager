import flask
import queue
import threading
import time
from logging_utils import root_logger
from werkzeug.serving import WSGIRequestHandler

# 单例：接收输入的主线程
WSGIRequestHandler.protocol_version = "HTTP/1.1"
app = flask.Flask(__name__)
serv_q = {
    "face_detection": queue.Queue(10),
    "face_alignment": queue.Queue(10)
}

# 模拟数据库
registered_services = ["face_detection", "face_alignment"]
services_info = {
    "face_detection": {
        "127.0.0.1:5500": {
            "cpu": 1,
            "mem": 1,
            "url": "http://127.0.0.1:5500/execute_task/face_detection"
        }
    },
    "face_alignment": {
        "127.0.0.1:5500": {
            "cpu": 1,
            "mem": 1,
            "url": "http://127.0.0.1:5500/execute_task/face_alignment"
        }
    }
}

def cal(serv_name, input_ctx):
    output_ctx = dict()
    if serv_name == "face_detection":
        assert "image" in input_ctx.keys()
        output_ctx["bbox"] = [[1,1,3,3],[4,4,7,7],[13,15,30,30],[20,27,35,35]]
        output_ctx["prob"] = [0.1,0.2,0.3,0.4]
    if serv_name == "face_alignment":
        assert "image" in input_ctx.keys()
        assert "bbox" in input_ctx.keys()
        assert "prob" in input_ctx.keys()
        output_ctx["head_pose"] = [[0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4]]
    
    return output_ctx

@app.route("/get_service_list", methods=["GET"])
def get_service_list_cbk():
    return flask.jsonify(list(services_info.keys()))

@app.route("/get_execute_url/<serv_name>", methods=["GET"])
def get_service_dict(serv_name):
    if serv_name not in services_info.keys():
        return flask.Response("{'msg': serv_name '{}' not registered}".format(serv_name),
                              status=500,
                              mimetype="application/json")
    return flask.jsonify(services_info[serv_name])

@app.route("/execute_task/<serv_name>", methods=["POST"])
def get_serv_cbk(serv_name):
    if serv_name not in services_info.keys():
        return flask.jsonify({"status": 1, "error": "unregistered services"})
    
    input_ctx = flask.request.json
    # TODO：进程池处理请求并返回
    output_ctx = cal(serv_name, input_ctx)
    return flask.jsonify(output_ctx)

def start_serv_listener(serv_port=9000):
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    
    app.run(host="0.0.0.0", port=serv_port)

if __name__ == "__main__":
    # 背景线程：对外接收输入数据，提供计算服务
    threading.Thread(target=start_serv_listener, args=(5500, ), daemon=True).start()

    # 服务线程：从任务队列获取数据，执行服务
    while True:
        time.sleep(4)
        root_logger.warning("sleep for 4 sec")

