import flask
import queue
import threading
import time
import json
from logging_utils import root_logger
from werkzeug.serving import WSGIRequestHandler

# 单例：接收输入的主线程
WSGIRequestHandler.protocol_version = "HTTP/1.1"
app = flask.Flask(__name__)


# 模拟数据库
registered_services = [
    "face_detection",
    "face_alignment",
    "car_detection",
    "helmet_detection"
]

cluster_info = {
    "127.0.0.1": {
        "node_role": "cloud",
        "n_cpu": 8,
        "cpu_ratio": 2.5,
        "mem": 4096 * 32,
        "mem_ratio": 0.3
    },
    "127.0.0.2": {
        "node_role": "edge",
        "n_cpu": 4,
        "cpu_ratio": 2.5,
        "mem": 4096,
        "mem_ratio": 0.3
    }
}

resource_info = {
    "host": {
        "127.0.0.1": {
            "face_detection": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            },
            "face_alignment": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            }
        }
    },
    "edge": {
        "127.0.0.1": {
            "face_detection": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            },
            "face_alignment": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            }
        }
    },
    "cloud": {
        "127.0.0.1": {
            "face_detection": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            },
            "face_alignment": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            }
        }        
    }
}

def cal(serv_name, input_ctx):
    output_ctx = dict()
    if serv_name == "face_detection":
        assert "image" in input_ctx.keys()
        output_ctx["bbox"] = [[1,1,3,3],[4,4,7,7],[13,15,30,30],[20,27,35,35]]
        output_ctx["prob"] = [0.1,0.2,0.3,0.4]
        time.sleep(1)
    if serv_name == "face_alignment":
        assert "image" in input_ctx.keys()
        assert "bbox" in input_ctx.keys()
        assert "prob" in input_ctx.keys()
        output_ctx["head_pose"] = [
            [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4],
            [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4]
        ]
        time.sleep(0.5)
    if serv_name == "car_detection":
        assert "image" in input_ctx.keys()
        output_ctx["result"] = {'truck': 2, 'car': 6}
        time.sleep(1)
    if serv_name == "helmet_detection":
        assert "clip" in input_ctx.keys()
        output_ctx["clip_result"] = list()
        for i in range(len(input_ctx["clip"])):
            output_ctx["clip_result"].append({'ntotal': 5, 'n_no_helmet': 1})
        time.sleep(2)
    
    return output_ctx

@app.route("/get_service_list", methods=["GET"])
def get_service_list_cbk():
    return flask.jsonify(registered_services)

@app.route("/get_resource_info", methods=["GET"])
def get_resource_info_cbk():
    return flask.jsonify(resource_info)

@app.route("/get_cluster_info", methods=["GET"])
def get_cluster_info_cbk():
    return flask.jsonify(cluster_info)

@app.route("/execute_task/<serv_name>", methods=["POST"])
def get_serv_cbk(serv_name):
    if serv_name not in registered_services:
        return flask.jsonify({"status": 1, "error": "unregistered services"})
    
    input_ctx = flask.request.json
    root_logger.info("request content-length={}(Bytes)".format(flask.request.headers.get('Content-Length')))
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

