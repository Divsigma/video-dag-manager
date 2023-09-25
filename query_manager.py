import cv2
import numpy
import flask
import flask.logging
import flask_cors
import random
import requests
import threading
import multiprocessing as mp
import queue
import time
import functools
import argparse
from werkzeug.serving import WSGIRequestHandler

import field_codec_utils
from logging_utils import root_logger
import logging_utils

import common

class Query():

    def __init__(self, query_id, node_addr, video_id, pipeline, user_constraint):
        self.query_id = query_id
        # 查询指令信息
        self.node_addr = node_addr
        self.video_id = video_id
        self.pipeline = pipeline
        self.user_constraint = user_constraint
        self.flow_mapping = None
        self.video_conf = None
        # NOTES: 目前仅支持流水线
        assert isinstance(self.pipeline, list)
        # 查询指令结果
        self.result = None
        # 查询指令运行时情境
        self.current_runtime = dict()
        # 历史记录
        self.plan_list = []
        self.runtime_list = []

    # ---------------------------------------
    # ---- 属性 ----
    def set_plan(self, video_conf, flow_mapping):
        while len(self.plan_list) >= QueryManager.LIST_BUFFER_SIZE_PER_QUERY:
            print("len(self.plan_list)={}".format(len(self.plan_list)))
            del self.plan_list[0]
        self.plan_list.append(self.get_plan())

        self.flow_mapping = flow_mapping
        self.video_conf = video_conf
        assert isinstance(self.flow_mapping, dict)
        assert isinstance(self.video_conf, dict)

    def get_plan(self):
        return {
            common.PLAN_KEY_VIDEO_CONF: self.video_conf,
            common.PLAN_KEY_FLOW_MAPPING: self.flow_mapping
        }
    
    def set_runtime(self, runtime_info):
        while len(self.runtime_list) >= QueryManager.LIST_BUFFER_SIZE_PER_QUERY:
            print("len(self.runtime_list)={}".format(len(self.runtime_list)))
            del self.runtime_list[0]
        self.runtime_list.append(self.current_runtime)

        self.current_runtime = runtime_info
    
    def get_runtime(self):
        return self.current_runtime
    
    def set_plan_and_runtime(self, video_conf, flow_mapping, runtime_info):
        if not runtime_info:
            runtime_info = dict()
        self.set_plan(video_conf=video_conf, flow_mapping=flow_mapping)
        self.set_runtime(runtime_info=runtime_info)

    def set_user_constraint(self, user_constraint):
        self.user_constraint = user_constraint
        assert isinstance(user_constraint, dict)

    def get_user_constraint(self):
        return self.user_constraint
    
    def get_query_id(self):
        return self.query_id
    
    def update_result(self, new_result):
        '''
        更新query的处理结果。
        该函数由query对应的job通过RESTFUL API触发，参见/query/sync_result接口
        '''
        if not self.result:
            self.result = {
                common.SYNC_RESULT_KEY_APPEND: list(),
                common.SYNC_RESULT_KEY_LATEST: dict()
            }
        assert isinstance(self.result, dict)

        for k, v in new_result.items():
            assert k in self.result.keys()
            if k == common.SYNC_RESULT_KEY_APPEND:
                # 仅保留最近一批结果（防止爆内存）
                if len(self.result[k]) > QueryManager.LIST_BUFFER_SIZE_PER_QUERY:
                    del self.result[k][0]
                self.result[k].append(v)

                # 更新runtime和plan
                if isinstance(v, dict):
                    assert(common.SYNC_RESULT_KEY_PLAN in v.keys())
                    assert(common.SYNC_RESULT_KEY_RUNTIME in v.keys())
                    self.set_plan_and_runtime(
                        video_conf=v[common.SYNC_RESULT_KEY_PLAN][common.PLAN_KEY_VIDEO_CONF],
                        flow_mapping=v[common.SYNC_RESULT_KEY_PLAN][common.PLAN_KEY_FLOW_MAPPING],
                        runtime_info=v[common.SYNC_RESULT_KEY_RUNTIME]
                    )
            elif k == common.SYNC_RESULT_KEY_LATEST:
                # 直接替换结果
                assert isinstance(v, dict)
                self.result[k].update(v)
            else:
                root_logger.error("unsupported sync result key: {}. value is: {}".format(k, v))
    
    def get_result(self):
        return self.result

class QueryManager():
    # 保存执行结果的缓冲大小
    LIST_BUFFER_SIZE_PER_QUERY = 10

    def __init__(self):
        self.global_query_count = 0
        self.service_cloud_addr = None
        self.query_dict = dict()
        self.video_info = dict()

        # keepalive的http客户端
        self.sess = requests.Session()

    def generate_global_job_id(self):
        self.global_query_count += 1
        new_id = "GLOBAL_ID_" + str(self.global_query_count)
        return new_id

    def set_service_cloud_addr(self, addr):
        self.service_cloud_addr = addr

    def add_video(self, node_addr, video_id, video_type):
        if node_addr not in self.video_info:
            self.video_info[node_addr] = dict()
            
        if video_id not in self.video_info[node_addr]:
            self.video_info[node_addr][video_id] = dict()

        self.video_info[node_addr][video_id].update({"type": video_type})

    def submit_query(self, query_id, node_addr, video_id, pipeline, user_constraint):
        # 在本地启动新的job
        assert query_id not in self.query_dict.keys()
        query = Query(query_id=query_id,
                      node_addr=node_addr,
                      video_id=video_id,
                      pipeline=pipeline,
                      user_constraint=user_constraint)
        # job.set_manager(self)
        self.query_dict[query.get_query_id()] = query
        root_logger.info("current query_dict={}".format(self.query_dict.keys()))

    def sync_query_result(self, query_id, new_result):
        assert query_id in self.query_dict

        query = self.query_dict[query_id]
        assert isinstance(query, Query)
        query.update_result(new_result)
    
    def get_query_result(self, query_id):
        assert query_id in self.query_dict

        query = self.query_dict[query_id]
        assert isinstance(query, Query)
        return query.get_result()
    
    def get_query_plan(self, query_id):
        assert query_id in self.query_dict

        query = self.query_dict[query_id]
        assert isinstance(query, Query)
        return query.get_plan()

    def get_query_runtime(self, query_id):
        assert query_id in self.query_dict

        query = self.query_dict[query_id]
        assert isinstance(query, Query)
        return query.get_runtime()









# 单例变量：主线程任务管理器，Manager
# manager = Manager()
query_manager = QueryManager()
# 单例变量：后台web线程
flask.Flask.logger_name = "listlogger"
WSGIRequestHandler.protocol_version = "HTTP/1.1"
query_app = flask.Flask(__name__)
flask_cors.CORS(query_app)

# 模拟云端数据库，维护接入节点及其已经submit的任务的job_uid。
# 用户接口（/user/xxx）争用查询&修改，云端调度器（cloud_scheduler_loop）争用查询
# 单例变量：接入到当前节点的节点信息
node_status = dict()








# 接受用户提交视频流查询
# 递归请求：/job/submit_job
@query_app.route("/query/submit_query", methods=["POST"])
@flask_cors.cross_origin()
def user_submit_query_cbk():
    # 获取用户针对视频流提交的job，转发到对应边端
    para = flask.request.json
    root_logger.info("/query/submit_query got para={}".format(para))
    node_addr = para['node_addr']
    video_id = para['video_id']
    pipeline = para['pipeline']
    user_constraint = para['user_constraint']

    if node_addr not in query_manager.video_info:
        return flask.jsonify({"status": 1, "error": "cannot found {}".format(node_addr)})

    # TODO：在云端注册任务实例，维护job执行结果、调度信息
    job_uid = query_manager.generate_global_job_id()
    new_job_info = {
        'job_uid': job_uid,
        'node_addr': node_addr,
        'video_id': video_id,
        'pipeline': pipeline,
        'user_constraint': user_constraint
    }
    query_manager.submit_query(query_id=new_job_info['job_uid'],
                                node_addr=new_job_info['node_addr'],
                                video_id=new_job_info['video_id'],
                                pipeline=new_job_info['pipeline'],
                                user_constraint=new_job_info['user_constraint'])

    # TODO：在边缘端为每个query创建一个job
    r = query_manager.sess.post("http://{}/job/submit_job".format(node_addr), 
                                json=new_job_info)
    
    # TODO：更新sidechan信息
    # cloud_ip = manager.get_cloud_addr().split(":")[0]
    cloud_ip = "127.0.0.1"
    r_sidechan = query_manager.sess.post(url="http://{}:{}/user/update_node_addr".format(cloud_ip, 5100),
                                   json={"job_uid": job_uid,
                                         "node_addr": node_addr.split(":")[0] + ":5101"})

    return flask.jsonify({"status": 0,
                          "msg": "submitted to (cloud) manager from api: /query/submit_query",
                          "query_id": job_uid,
                          "r_sidechan": r_sidechan.text})

# TODO：同步job的执行结果
@query_app.route("/query/sync_result", methods=["POST"])
@flask_cors.cross_origin()
def query_sync_result_cbk():
    para = flask.request.json

    job_uid = para['job_uid']
    job_result = para['job_result']

    query_manager.sync_query_result(query_id=job_uid, new_result=job_result)

    return flask.jsonify({"status": 500})

@query_app.route("/query/get_result/<query_id>", methods=["GET"])
@flask_cors.cross_origin()
def query_get_result_cbk(query_id):
    return flask.jsonify(query_manager.get_query_result(query_id))

@query_app.route("/query/get_plan/<query_id>", methods=["GET"])
@flask_cors.cross_origin()
def query_get_plan_cbk(query_id):
    return flask.jsonify(query_manager.get_query_plan(query_id))
@query_app.route("/query/get_runtime/<query_id>", methods=["GET"])
@flask_cors.cross_origin()
def query_get_runtime_cbk(query_id):
    return flask.jsonify(query_manager.get_query_runtime(query_id))

@query_app.route("/query/get_agg_info/<query_id>", methods=["GET"])
@flask_cors.cross_origin()
def query_get_agg_info_cbk(query_id):
    resp = dict()
    resp.update(query_manager.get_query_result(query_id))

    # resp["latest_result"] = dict()
    # resp["latest_result"]["plan"] = query_manager.get_query_plan(query_id)
    # resp["latest_result"]["runtime"] = query_manager.get_query_runtime(query_id)
    return flask.jsonify(resp)

@query_app.route("/node/get_video_info", methods=["GET"])
@flask_cors.cross_origin()
def node_video_info():
    return flask.jsonify(query_manager.video_info)

# 接受边缘节点的视频流接入信息
@query_app.route("/node/join", methods=["POST"])
@flask_cors.cross_origin()
def node_join_cbk():
    para = flask.request.json
    root_logger.info("from {}: got {}".format(flask.request.remote_addr, para))
    node_ip = flask.request.remote_addr
    node_port = para['node_port']
    node_addr = node_ip + ":" + str(node_port)
    video_id = para['video_id']
    video_type = para['video_type']

    query_manager.add_video(node_addr=node_addr, video_id=video_id, video_type=video_type)

    return flask.jsonify({"status": 0, "msg": "joined one video to query_manager", "node_addr": node_addr})








def start_query_listener(serv_port=5000):
    query_app.run(host="0.0.0.0", port=serv_port)

# 云端调度器主循环：为manager的所有任务决定调度策略，并主动post策略到对应节点，让节点代理执行
# 不等待执行结果，节点代理执行完毕后post /job/update_plan接口提交结果
def cloud_scheduler_loop(query_manager=None):
    assert query_manager
    assert isinstance(query_manager, QueryManager)

    # import scheduler_func.demo_scheduler
    # import scheduler_func.pid_scheduler
    # import scheduler_func.pid_mogai_scheduler
    # import scheduler_func.pid_content_aware_scheduler
    import scheduler_func.lat_first_pid


    while True:
        # 每5s调度一次
        time.sleep(3)

        root_logger.info("start new schedule ...")
        try:
            # 获取资源情境
            r = query_manager.sess.get(
                url="http://{}/get_resource_info".format(query_manager.service_cloud_addr))
            resource_info = r.json()
            
            # 访问已注册的所有job实例，获取实例中保存的结果，生成调度策略
            query_dict = query_manager.query_dict.copy()
            for qid, query in query_dict.items():
                assert isinstance(query, Query)

                query_id = query.query_id
                node_addr = query.node_addr
                user_constraint = query.user_constraint
                assert node_addr

                # 获取当前query的运行时情境（query_id == job_uid
                # r = query_manager.sess.get(
                #     url="http://{}/job/get_runtime/{}".format(node_addr, query_id)
                # )
                # runtime_info = r.json()
                runtime_info = query.get_runtime()

                # conf, flow_mapping = scheduler_func.pid_mogai_scheduler.scheduler(
                # conf, flow_mapping = scheduler_func.pid_content_aware_scheduler.scheduler(
                conf, flow_mapping = scheduler_func.lat_first_pid.scheduler(
                    # flow=job.get_dag_flow(),
                    job_uid=query_id,
                    dag={"generator": "x", "flow": query.pipeline},
                    resource_info=resource_info,
                    runtime_info=runtime_info,
                    # last_plan_res=last_plan_result,
                    user_constraint=user_constraint
                )

                # # 更新运行时情境和查询策略（以便用户从云端获取）
                # query.set_plan_and_runtime(video_conf=conf, flow_mapping=flow_mapping, runtime_info=runtime_info)

                # 主动post策略到对应节点（即更新对应视频流query pipeline的执行策略），让节点代理执行，不等待执行结果
                r = query_manager.sess.post(url="http://{}/job/update_plan".format(node_addr),
                            json={"job_uid": query_id, "video_conf": conf, "flow_mapping": flow_mapping})
        # except AssertionError as e:
        #     root_logger.error("caught assertion, msg={}".format(e), exc_info=True)
        except Exception as e:
            root_logger.error("caught exception, type={}, msg={}".format(repr(e), e), exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query_port', dest='query_port',
                        type=int, default=5000)
    parser.add_argument('--serv_cloud_addr', dest='serv_cloud_addr',
                        type=str, default='127.0.0.1:5500')
    args = parser.parse_args()

    threading.Thread(target=start_query_listener,
                     args=(args.query_port,),
                     name="QueryFlask",
                     daemon=True).start()
    
    time.sleep(1)

    query_manager.set_service_cloud_addr(addr=args.serv_cloud_addr)

    # 启动视频流sidechan（由云端转发请求到边端）
    import cloud_sidechan
    video_serv_inter_port = 5100
    mp.Process(target=cloud_sidechan.init_and_start_video_proc,
               args=(video_serv_inter_port,)).start()
    time.sleep(1)

    cloud_scheduler_loop(query_manager)