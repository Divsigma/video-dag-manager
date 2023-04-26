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
    "D": queue.Queue(10),
    "C": queue.Queue(10)
}

# 模拟数据库
registered_services = ["D", "C"]

def cal(serv_name, input_ctx):
    output_ctx = dict()
    if serv_name == "D":
        assert "image" in input_ctx.keys()
        output_ctx["bbox"] = [[1,1,3,3],[4,4,7,7],[13,15,30,30],[20,27,35,35]]
        output_ctx["prob"] = [0.1,0.2,0.3,0.4]
    if serv_name == "C":
        assert "image" in input_ctx.keys()
        assert "bbox" in input_ctx.keys()
        assert "prob" in input_ctx.keys()
        output_ctx["head_pose"] = [[0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4], [0.1,0.2,0.4]]
    
    return output_ctx

@app.route("/get_serv/<serv_name>", methods=["POST"])
def get_serv_cbk(serv_name):
    if serv_name not in registered_services:
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
    threading.Thread(target=start_serv_listener, args=(9000, ), daemon=True).start()

    # 服务线程：从任务队列获取数据，执行服务
    while True:
        time.sleep(4)
        root_logger.warning("sleep for 4 sec")

