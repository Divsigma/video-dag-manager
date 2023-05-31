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

# SingleFrameGenerator的数据生成函数
def sfg_get_next_init_task(video_cap=None, video_conf=None):
    # # 模拟产生数据
    # frame = list()
    # for i in range(10):
    #     frame.append(list())
    #     for j in range(3):
    #         frame[i].append(random.randint(1,1000))
    # import numpy
    # frame = numpy.array(frame)

    assert video_cap

    global resolution_wh

    # 从视频流读取一帧
    ret, frame = video_cap.read()
    assert ret

    # 根据video_conf['resolution']调整大小
    frame = cv2.resize(frame, (
        resolution_wh[video_conf['resolution']]['w'],
        resolution_wh[video_conf['resolution']]['h']
    ))

    input_ctx = dict()
    # input_ctx['image'] = (video_cap.get(cv2.CAP_PROP_POS_FRAMES), numpy.array(frame).shape)
    st_time = time.time()
    input_ctx['image'] = field_codec_utils.encode_image(frame)
    ed_time = time.time()
    root_logger.info(
        "time consumed in encode-decode: {}".format(ed_time - st_time))
    # input_ctx['image'] = frame.tolist()

    root_logger.warning(
        "only unsupport init task with one image frame as input")

    return input_ctx

class JobManager():
    # 保存执行结果的缓冲大小
    LIST_BUFFER_SIZE = 10

    def __init__(self):
        self.cloud_addr = None
        self.local_addr = None

        # 计算服务url
        self.service_cloud_addr = None
        self.service_url = dict()

        # keepalive的http客户端：用于对query manager通信
        self.sess = requests.Session()

        # 本地视频流
        self.video_info_list = [
            {"id": 0, "type": "student in classroom", "url": "input/input.mov"},
            {"id": 1, "type": "people in meeting-room", "url": "input/input1.mp4"},
            {"id": 3, "type": "traffic flow outdoor", "url": "input/traffic-720p.mp4"}
        ]

        # 模拟数据库：记录下发到本地的job以及该job的执行结果
        self.job_dict = dict()
        # self.job_result_dict = dict()

    def set_service_cloud_addr(self, addr):
        self.service_cloud_addr = addr
    
    # 接入query manager，汇报自身信息
    def join_query_controller(self, query_addr, tracker_port):
        self.query_addr = query_addr
        for video_info in self.video_info_list:
            r = self.sess.post(url="http://{}/node/join".format(self.query_addr),
                               json={"node_port": tracker_port,
                                     "video_id": video_info["id"],
                                     "video_type": video_info["type"]})
            self.local_addr = r.json()["node_addr"]

    def get_video_info_by_id(self, video_id=id):
        for info in self.video_info_list:
            if info["id"] == video_id:
                return info
        return None
    
    # 获取计算服务url
    def get_chosen_service_url(self, taskname, choice):
        port = self.service_cloud_addr.split(':')[1]
        url = "http://{}:{}/execute_task/{}".format(
            choice["node_ip"], port, taskname)
        return url

    # 更新调度计划：与通信进程竞争self.job_dict[job.get_job_uid()]，修改job状态
    def update_job_plan(self, job_uid, video_conf, flow_mapping):
        assert job_uid in self.job_dict.keys()

        job = self.job_dict[job_uid]
        assert isinstance(job, Job)
        job.set_plan(video_conf=video_conf, flow_mapping=flow_mapping)

        root_logger.info(
            "updated job-{} plan".format(job.get_job_uid()))

    # 在本地启动新的job
    def submit_job(self, job_uid, node_addr, video_id, pipeline, user_constraint):
        assert job_uid not in self.job_dict.keys()
        job = Job(job_uid=job_uid,
                  node_addr=node_addr,
                  video_id=video_id,
                  pipeline=pipeline,
                  user_constraint=user_constraint)
        job.set_manager(self)
        self.job_dict[job.get_job_uid()] = job
        root_logger.info("current job_dict={}".format(self.job_dict.keys()))

    # 工作节点获取未分配工作线程的查询任务
    def start_new_job(self):
        root_logger.info("job_dict keys: {}".format(self.job_dict.keys()))

        n = 0
        for job in self.job_dict:
            assert isinstance(job, Job)
            if job.get_state() == Job.JOB_STATE_READY:
                n += 1
                job.start_worker_loop()
                root_logger.info("run job-{} in new thread".format(job.get_job_uid()))
        
        if n == 0:
            root_logger.warning("no new job to start")
        
        root_logger.info("{} jobs running".format(len(self.job_dict)))

    # TODO：将Job的结果同步到query manager（本地不存放结果）
    def submit_job_result(self, job_uid, job_result, report2qm=True):
        # if job_uid not in self.job_result_dict:
        #     self.job_result_dict[job_uid] = {
        #         "appended_result": list(), "latest_result": dict()}
        # assert isinstance(job_result, dict)
        # assert job_uid in self.job_result_dict
        # for k, v in job_result.items():
        #     assert k in self.job_result_dict[job_uid].keys()
        #     if k == "appended_result":
        #         # 仅保留最近一批结果（防止爆内存）
        #         if len(self.job_result_dict[job_uid][k]) > JobManager.LIST_BUFFER_SIZE:
        #             del self.job_result_dict[job_uid][k][0]
        #         self.job_result_dict[job_uid][k].append(v)
        #     else:
        #         # 直接替换结果
        #         assert isinstance(v, dict)
        #         self.job_result_dict[job_uid][k].update(v)

        if report2qm:
            r = self.sess.post(url="http://{}/query/sync_result".format(self.query_addr),
                               json={"job_uid": job_uid,
                                     "job_result": job_result})

    def remove_job(self, job):
        # 根据job的id移除job
        del self.job_dict[job.get_job_uid()]


class Job():
    JOB_STATE_READY = 0
    JOB_STATE_RUNNING = 1

    def __init__(self, job_uid, node_addr, video_id, pipeline, user_constraint):
        # job的全局唯一id
        self.job_uid = job_uid
        self.manager = None
        # 视频分析流信息
        self.node_addr = node_addr
        self.video_id = video_id
        self.pipeline = pipeline
        # 执行状态机
        self.state = Job.JOB_STATE_READY
        self.worker_thread = None
        # 调度状态机：执行计划与历史计划的执行结果
        self.user_constraint = user_constraint
        self.flow_mapping = None
        self.video_conf = None
        # keepalive的http客户端：用于请求计算服务
        self.sess = requests.Session()

        # 拓扑解析dag图
        # NOTES: 目前仅支持流水线
        #        Start -> D -> C -> End
        #          0      1    2     3
        assert isinstance(self.pipeline, list)

    def set_manager(self, manager):
        self.manager = manager
        assert isinstance(self.manager, JobManager)

    def get_job_uid(self):
        return self.job_uid
    
    def get_job_state(self):
        return self.state

    # ---------------------------------------
    # ---- 执行计划与执行计划结果的相关函数 ----
    def set_plan(self, video_conf, flow_mapping):
        self.flow_mapping = flow_mapping
        self.video_conf = video_conf
        assert isinstance(self.flow_mapping, dict)
        assert isinstance(self.video_conf, dict)

    def get_plan(self):
        return {"video_conf": self.video_conf, "flow_mapping": self.flow_mapping}

    def set_user_constraint(self, user_constraint):
        self.user_constraint = user_constraint
        assert isinstance(user_constraint, dict)

    def get_user_constraint(self):
        return self.user_constraint
    
    # ------------------
    # ---- 执行循环 ----
    def start_worker_loop(self):
        self.worker_thread = threading.Thread(target=self.worker_loop)
        self.worker_thread.start()
        self.state = Job.JOB_STATE_RUNNING

    def worker_loop(self):
        assert isinstance(self.manager, JobManager)

        # 0、初始化数据流来源（TODO：从缓存区读取）
        cap = cv2.VideoCapture(self.manager.get_video_info_by_id(self.video_id)['url'])

        while True:
            # 1、根据video_conf，获取本次循环的输入数据（TODO：从缓存区读取）
            output_ctx = sfg_get_next_init_task(video_cap=cap, video_conf=self.video_conf)
            root_logger.info("done generator task, get_next_init_task({})".format(output_ctx.keys()))
            
            # 2、执行
            plan_result = dict()
            plan_result['delay'] = dict()
            for taskname in self.pipeline:

                root_logger.info("to forward taskname={}".format(taskname))

                input_ctx = output_ctx
                root_logger.info("get input_ctx({}) of taskname({})".format(
                    input_ctx.keys(),
                    taskname
                ))

                # 根据flow_mapping，执行task，并记录中间结果
                root_logger.info("flow_mapping ={}".format(self.flow_mapping))
                choice = self.flow_mapping[taskname]
                root_logger.info("get choice of '{}' in flow_mapping, choose: {}".format(taskname, choice))
                url = self.manager.get_chosen_service_url(taskname, choice)
                root_logger.info("get url {}".format(url))

                st_time = time.time()
                output_ctx = self.invoke_service(serv_url=url, taskname=taskname, input_ctx=input_ctx)
                ed_time = time.time()
                root_logger.info("got service result: {}, (delta_t={})".format(
                                  output_ctx.keys(), ed_time - st_time))
                plan_result['delay'][taskname] = ed_time - st_time

            # 3、TODO：记录结果，并通过job manager同步结果到query manager
            self.manager.submit_job_result(job_uid=self.get_job_uid(),
                                           job_result={
                                               "appended_result": output_ctx,
                                               "latest_result": {
                                                   "plan": self.get_plan(),
                                                   "plan_result": plan_result
                                                }
                                            })


    def invoke_service(self, serv_url, taskname, input_ctx):
        root_logger.info("get serv_url={}".format(serv_url))

        r = self.sess.post(url=serv_url, json=input_ctx)

        try:
            return r.json()

        except Exception as e:
            root_logger.error("caught exception: {}".format(e), exc_info=True)
            root_logger.error("got serv result: {}".format(r.text))
            return None








# 单例变量：主线程任务管理器，Manager
job_manager = JobManager()
# 单例变量：后台web线程
flask.Flask.logger_name = "listlogger"
WSGIRequestHandler.protocol_version = "HTTP/1.1"
tracker_app = flask.Flask(__name__)
flask_cors.CORS(tracker_app)








# 接受query manager下发的query，生成本地job（每个query一个job、每个job一个线程）
@tracker_app.route("/job/submit_job", methods=["POST"])
@flask_cors.cross_origin()
def job_submit_job_cbk():
    # 获取产生job的初始化参数
    para = flask.request.json
    job_manager.submit_job(job_uid=para['job_uid'],
                           node_addr=para['node_addr'],
                           video_id=para['video_id'],
                           pipeline=para['pipeline'],
                           user_constraint=['user_constraint'])
    return flask.jsonify({"status": 0,
                          "msg": "submitted to manager from api: node/submit_job",
                          "job_uid": para["job_uid"]})

# 接受调度计划更新
@tracker_app.route("/job/update_plan", methods=["POST"])
@flask_cors.cross_origin()
def node_update_plan_cbk():
    para = flask.request.json
    root_logger.info("/job/update_plan got para={}".format(para))

    # 与工作节点模拟CPU执行的主循环竞争manager
    job_manager.update_job_plan(job_uid=para['job_uid'],
                                video_conf=para['video_conf'],
                                flow_mapping=para['flow_mapping'])

    return flask.jsonify({"status": 0, "msg": "node updated plan (manager.update_job_plan)"})








def start_tracker_listener(serv_port=5001):
    tracker_app.run(host="0.0.0.0", port=serv_port)
    # app.run(port=serv_port)
    # app.run(host="*", port=serv_port)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query_addr', dest='query_addr',
                        type=str, default='127.0.0.1:5000')
    parser.add_argument('--tracker_port', dest='tracker_port', 
                        type=int, default=5001)
    parser.add_argument('--serv_cloud_addr', dest='serv_cloud_addr',
                        type=str, default='127.0.0.1:5500')
    args = parser.parse_args()

    # 接受下发的query生成job、接收更新的调度策略
    threading.Thread(target=start_tracker_listener,
                    args=(args.tracker_port,),
                    name="TrackerFlask",
                    daemon=True).start()

    time.sleep(1)
    
    # 接入query manger
    job_manager.join_query_controller(query_addr=args.query_addr,
                                      tracker_port=args.tracker_port)
    root_logger.info("joined to query controller")
    
    job_manager.set_service_cloud_addr(addr=args.serv_cloud_addr)

    # 线程轮询启动循环
    # 一个Job对应一个视频流查询、对应一个进程/线程
    while True:
        
        job_manager.start_new_job()

        sleep_sec = 5
        root_logger.warning(f"---- sleeping for {sleep_sec} sec ----")
        time.sleep(sleep_sec)