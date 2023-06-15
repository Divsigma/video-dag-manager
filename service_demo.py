import flask
import queue
import threading
import time
import random
import json
from logging_utils import root_logger
from werkzeug.serving import WSGIRequestHandler
import argparse

# 单例：接收输入的主线程
WSGIRequestHandler.protocol_version = "HTTP/1.1"
app = flask.Flask(__name__)

# 模拟数据库
services_args = {
    "face_detection": {
        'net_type': 'mb_tiny_RFB_fd',
        'input_size': 480,
        'threshold': 0.7,
        'candidate_size': 1500,
        'device': 'cpu'
        # 'device': 'cuda:0'
    },
    "face_alignment": {
        # 'lite_version': False,
        # 'model_path': 'models/hopenet.pkl',
        'lite_version': True,
        'model_path': 'models/hopenet_lite_6MB.pkl',
        'batch_size': 1,
        # 'device': 'cuda:0'
        'device': 'cpu'
    },
    "car_detection": {
        'weights': 'yolov5s.pt',
        # 'device': 'cuda:0'
        'device': 'cpu'
    }
}

import field_codec_utils
import services.headup_detect.face_detection
import services.headup_detect.face_alignment_cnn
import services.car_detection.car_detection

registered_services = {
    "face_detection": services.headup_detect.face_detection.FaceDetection(services_args["face_detection"]),
    "face_alignment": services.headup_detect.face_alignment_cnn.FaceAlignmentCNN(services_args["face_alignment"]),
    "car_detection": services.car_detection.car_detection.CarDetection(services_args["car_detection"]),
    "helmet_detection": None
}

cluster_info = {
    "127.0.0.1": {
        "node_role": "cloud",
        "n_cpu": 8,
        "cpu_ratio": 2.5
    },
    "127.0.0.2": {
        "node_role": "edge",
        "n_cpu": 4,
        "cpu_ratio": 2.5
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

    st_time = time.time()

    if serv_name == "face_detection":
        # 解码
        assert "image" in input_ctx.keys()
        input_ctx["image"] = field_codec_utils.decode_image(input_ctx["image"])
        # 执行
        output_ctx = registered_services[serv_name](input_ctx)
        
        # 编码
        if "image" in output_ctx:
            output_ctx["image"] = field_codec_utils.encode_image(output_ctx["image"])
        if "faces" in output_ctx:
            for i in range(len(output_ctx["faces"])):
                face = output_ctx["faces"][i]
                output_ctx["faces"][i] = field_codec_utils.encode_image(face)
        # output_ctx["bbox"] = [[1,1,3,3],[4,4,7,7],[13,15,30,30],[20,27,35,35]]
        # output_ctx["prob"] = [0.1,0.2,0.3,0.4]
        # time.sleep(1)
    
    if serv_name == "face_alignment":
        assert "image" in input_ctx.keys() or "faces" in input_ctx.keys()
        assert "bbox" in input_ctx.keys()
        assert "prob" in input_ctx.keys()
        # 解码
        if "image" in input_ctx:
            input_ctx["image"] = field_codec_utils.decode_image(input_ctx["image"])
        if "faces" in input_ctx:
            for i in range(len(input_ctx["faces"])):
                face = input_ctx["faces"][i]
                input_ctx["faces"][i] = field_codec_utils.decode_image(face)
        # 执行
        output_ctx = registered_services[serv_name](input_ctx)

        # 编码
        if "image" in output_ctx:
            output_ctx["image"] = field_codec_utils.encode_image(output_ctx["image"])
        # output_ctx["head_pose"] = [
        #     [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4],
        #     [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4]
        # ]
        # time.sleep(0.5)
    
    if serv_name == "car_detection":
        assert "image" in input_ctx.keys()
        # 解码
        input_ctx["image"] = field_codec_utils.decode_image(input_ctx["image"])
        # 执行
        output_ctx = registered_services[serv_name](input_ctx)
        
        # 编码
        if "image" in output_ctx:
            output_ctx["image"] = field_codec_utils.encode_image(output_ctx["image"])
        # output_ctx["result"] = {'truck': 2, 'car': 6}
        # time.sleep(1)
    
    if serv_name == "helmet_detection":
        assert "clip" in input_ctx.keys()
        output_ctx["clip_result"] = list()
        for i in range(len(input_ctx["clip"])):
            output_ctx["clip_result"].append({'ntotal': 5, 'n_no_helmet': 1})
        time.sleep(2)
    
    ed_time = time.time()
    root_logger.info("serv {} toke {} sec".format(serv_name, ed_time - st_time))

    return output_ctx

@app.route("/get_service_list", methods=["GET"])
def get_service_list_cbk():
    return flask.jsonify(
        list(registered_services.keys())
    )

@app.route("/get_resource_info", methods=["GET"])
def get_resource_info_cbk():
    return flask.jsonify(resource_info)

@app.route("/get_cluster_info", methods=["GET"])
def get_cluster_info_cbk():
    global cluster_info

    resp_info = cluster_info.copy()
    for ip, info in cluster_info.items():
        resp_info[ip]["cpu_ratio"] = round(info["n_cpu"] - random.random(), 2)
    return flask.jsonify(resp_info)

@app.route("/execute_task/<serv_name>", methods=["POST"])
def get_serv_cbk(serv_name):
    if serv_name not in registered_services.keys():
        return flask.jsonify({"status": 1, "error": "unregistered services"})
    
    input_ctx = flask.request.json
    root_logger.info("request content-length={}(Bytes)".format(flask.request.headers.get('Content-Length')))
    # TODO：进程池处理请求并返回
    output_ctx = cal(serv_name, input_ctx)
    return flask.jsonify(output_ctx)

def start_serv_listener(serv_port=5500):
    WSGIRequestHandler.protocol_version = "HTTP/1.1"
    
    app.run(host="0.0.0.0", port=serv_port)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', dest='port',
                        type=int, default=5500)
    args = parser.parse_args()

    # 背景线程：对外接收输入数据，提供计算服务
    threading.Thread(target=start_serv_listener, args=(args.port, ), daemon=True).start()

    # 服务线程：从任务队列获取数据，执行服务
    while True:
        time.sleep(4)
        root_logger.warning("sleep for 4 sec")

