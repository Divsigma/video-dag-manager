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

class QueryManager():
    def __init__(self):
        self.global_query_count = 0

    def generate_global_job_id(self):
        self.global_query_count += 1
        new_id = "GLOBAL_ID_" + str(self.global_query_count)
        return new_id

class Manager():
    # 保存执行结果的缓冲大小
    LIST_BUFFER_SIZE = 10

    def __init__(self):
        self.cloud_addr = None
        self.local_addr = None

        # 计算服务url
        self.service_cloud_addr = None
        self.service_url = dict()

        # keepalive的http客户端
        self.sess = requests.Session()

        # 本地视频流
        self.video_info_list = [
            {"id": 0, "type": "student in classroom", "url": "input/input.mov"},
            {"id": 1, "type": "people in meeting-room", "url": "input/input1.mp4"},
            {"id": 3, "type": "traffic flow outdoor", "url": "input/traffic-720p.mp4"}
        ]

        # 模拟数据库：记录下发到本地的job以及该job的执行结果
        self.job_dict = dict()
        self.job_result_dict = dict()

    def set_cloud_addr(self, cloud_ip, cloud_port):
        self.cloud_addr = cloud_ip + ":" + str(cloud_port)

    def set_service_cloud_addr(self, addr):
        self.service_cloud_addr = addr

    def get_cloud_addr(self):
        return self.cloud_addr

    def get_video_info_by_id(self, video_id=id):
        for info in self.video_info_list:
            if info["id"] == video_id:
                return info
        return None

    def get_available_service_list(self):
        r = self.sess.get(
            url="http://{}/get_service_list".format(self.service_cloud_addr))
        assert isinstance(r.json(), list)
        return r.json()

    def get_service_dict(self, taskname):
        r = self.sess.get(url="http://{}/get_execute_url/{}".format(
            self.service_cloud_addr,
            taskname
        ))
        assert isinstance(r.json(), dict)
        return r.json()

    def get_chosen_service_url(self, taskname, choice):
        port = self.service_cloud_addr.split(':')[1]
        url = "http://{}:{}/execute_task/{}".format(
            choice["node_ip"], port, taskname)
        return url

    def join_cloud(self, local_port):
        # 接入云端，汇报自身信息
        for video_info in self.video_info_list:
            r = self.sess.post(url="http://{}/node/update_status".format(self.cloud_addr),
                               json={"node_port": local_port,
                                     "video_id": video_info["id"],
                                     "video_type": video_info["type"]})
            self.local_addr = r.json()["node_addr"]

    # 云端/user/submit_job：争用node_status，修改node_addr的job_uid和node关系
    # 云端调度器需要根据job_uid找到节点，更新node_addr的任务调度策略：争用node_status，查询node_addr
    # 云端/user/submit_job_constraint需要根据job_uid找到节点，更新node_addr的任务约束：争用node_status，查询node_addr
    def get_node_addr_by_job_uid(self, job_uid):
        for node_addr, info in node_status.items():
            assert "job_uid_list" in info
            for uid in info["job_uid_list"]:
                if job_uid == uid:
                    return node_addr
        root_logger.error(
            "cannot found job_uid-{} in node_status: {}".format(job_uid, node_status))
        return None

    def post_reschedule_request(self, job=None):
        assert isinstance(job, Job)
        url = "http://{}/node/get_plan".format(self.get_cloud_addr())
        param = {
            "job_uid": job.get_job_uid(),
            "dag": job.get_dag(),
            "last_plan_result": job.get_plan_result(),
            "user_constraint": job.get_user_constraint()
        }
        r = self.sess.post(url=url,
                           json=param)

        root_logger.info(
            "posted unsched_req for job-{} to cloud, got r={}".format(job.get_job_uid(), r.json()))

    # 工作节点更新调度计划：与通信进程竞争self.job_dict[job.get_job_uid()]，修改job状态
    def update_job_plan(self, job_uid, video_conf, flow_mapping):
        assert job_uid in self.job_dict.keys()

        job = self.job_dict[job_uid]
        assert isinstance(job, Job)
        job.set_plan(video_conf=video_conf, flow_mapping=flow_mapping)

        root_logger.info(
            "updated job-{} plan".format(job.get_job_uid()))

    def update_job_user_constraint(self, job_uid, user_constraint):
        assert job_uid in self.job_dict.keys()

        job = self.job_dict[job_uid]
        assert isinstance(job, Job)

        job.set_user_constraint(user_constraint=user_constraint)
        root_logger.info("set job-{} user_constraint to '{}'".format(
            job.get_job_uid(),
            job.get_user_constraint()
        ))

    def submit_job(self, job_uid, node_addr, video_id, pipeline, user_constraint):
        # 在本地启动新的job
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

    def submit_job_result(self, job_uid, job_result, report2cloud=False):
        # 将Job本次提交的结果同步到本地
        if job_uid not in self.job_result_dict:
            self.job_result_dict[job_uid] = {
                "appended_result": list(), "latest_result": dict()}
        assert isinstance(job_result, dict)
        assert job_uid in self.job_result_dict
        for k, v in job_result.items():
            assert k in self.job_result_dict[job_uid].keys()
            if k == "appended_result":
                # 仅保留最近一批结果（防止爆内存）
                if len(self.job_result_dict[job_uid][k]) > Manager.LIST_BUFFER_SIZE:
                    del self.job_result_dict[job_uid][k][0]
                self.job_result_dict[job_uid][k].append(v)
            else:
                # 直接替换结果
                assert isinstance(v, dict)
                self.job_result_dict[job_uid][k].update(v)

        # 将Job本次提交的结果同步到云端（注意/node/sync_job_result的处理，要避免死循环）
        if self.cloud_addr == self.local_addr:
            root_logger.warning(
                "{} post /node/sync_job_result to itself".format(self.local_addr))

        if report2cloud:
            r = self.sess.post(url="http://{}/node/sync_job_result".format(self.cloud_addr),
                               json={"job_uid": job_uid,
                                     "job_result": job_result})

    def get_job_result(self, job_uid):
        if job_uid in self.job_result_dict:
            return self.job_result_dict[job_uid]
        return None

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
        # keepalive的http客户端
        self.sess = requests.Session()

        # 拓扑解析dag图
        # NOTES: 目前仅支持流水线
        #        Start -> D -> C -> End
        #          0      1    2     3
        assert isinstance(self.pipeline, list)

    def set_manager(self, manager):
        self.manager = manager
        assert isinstance(self.manager, Manager)

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
        # 0、初始化数据流来源（TODO：从缓存区读取）
        cap = cv2.VideoCapture(self.manager.get_video_info_by_id(self.video_id)['url'])

        while True:
            # 1、根据video_conf，获取本次循环的输入数据（TODO：从缓存区读取）
            output_ctx = sfg_get_next_init_task(video_cap=cap, video_conf=self.video_conf)
            root_logger.info("done generator task, get_next_init_task({})".format(output_ctx.keys()))
            
            # 2、执行
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

            # 3、通过manager，向云端汇总结果
            self.manager.submit_job_result(job_uid=self.get_job_uid(),
                                           job_result={
                                               "appended_result": job.get_latest_loop_count_result(),
                                               "latest_result": {
                                                   "plan": self.get_plan(),
                                                   "plan_result": self.get_plan_result()
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
manager = Manager()
query_manager = QueryManager()
# 单例变量：后台web线程
flask.Flask.logger_name = "listlogger"
WSGIRequestHandler.protocol_version = "HTTP/1.1"
user_app = flask.Flask(__name__)
tracker_app = flask.Flask(__name__)
flask_cors.CORS(user_app)
flask_cors.CORS(tracker_app)

# 模拟云端数据库，维护接入节点及其已经submit的任务的job_uid。
# 用户接口（/user/xxx）争用查询&修改，云端调度器（cloud_scheduler_loop）争用查询
# 单例变量：接入到当前节点的节点信息
node_status = dict()


# 外部接口：从云端接受用户提交的job参数，包括指定的DAG、数据来源，将/node/submit_job的注册结果返回
@user_app.route("/user/submit_query", methods=["POST"])
@flask_cors.cross_origin()
def user_submit_query_cbk():
    # 获取用户针对视频流提交的job，转发到对应边端
    para = flask.request.json
    root_logger.info("/user/submit_query got para={}".format(para))
    node_addr = para['node_addr']
    video_id = para['video_id']
    pipeline = para['pipeline']
    user_constraint = para['user_constraint']

    if node_addr not in node_status:
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

    # 在云端维护job_uid和节点关系
    if "job_uid_list" not in node_status[node_addr]:
        node_status[node_addr]["job_uid_list"] = list()
    node_status[node_addr]["job_uid_list"].append(job_uid)

    # TODO：在边缘端为每个query创建一个job
    r = query_manager.sess.post("http://{}/node/submit_job".format(node_addr), 
                          json=new_job_info)

    return flask.jsonify({"status": 0,
                          "msg": "submitted to (cloud) manager from api: /user/submit_job",
                          "job_uid": job_uid})

@user_app.route("/user/sync_job_result/<job_uid>", methods=["GET"])
@flask_cors.cross_origin()
def user_sync_job_result_cbk(job_uid):
    job_result = manager.get_job_result(job_uid=job_uid)
    return flask.jsonify({"status": 0,
                          "result": job_result})

# 外部接口：获取当前节点所知的所有节点信息
@user_app.route("/user/get_all_status")
@flask_cors.cross_origin()
def user_get_all_status_cbk():
    return flask.jsonify({"status": 0, "data": node_status})


# 内部接口：节点间同步job的执行结果到本地
@tracker_app.route("/node/sync_job_result", methods=["POST"])
@flask_cors.cross_origin()
def node_sync_job_result_cbk():
    para = flask.request.json

    # NOTES：防止再次请求/node/sync_job_result接口，否则死锁
    manager.submit_job_result(job_uid=para["job_uid"],
                              job_result=para["job_result"],
                              report2cloud=False)

    return flask.jsonify({"status": 200})

# 内部接口：接受其他节点传入的job初始化参数，在本地生成可以执行的job


@tracker_app.route("/node/submit_job", methods=["POST"])
@flask_cors.cross_origin()
def node_submit_job_cbk():
    # 获取产生job的初始化参数
    para = flask.request.json
    manager.submit_job(job_uid=para['job_uid'],
                       node_addr=para['node_addr'],
                       video_id=para['video_id'],
                       pipeline=para['pipeline'],
                       user_constraint=['user_constraint'])
    return flask.jsonify({"status": 0,
                          "msg": "submitted to manager from api: node/submit_job",
                          "job_uid": para["job_uid"]})

# 内部接口：接收云端下发的任务约束，包括job_uid，时延和精度
@tracker_app.route("/node/submit_job_user_constraint", methods=["POST"])
@flask_cors.cross_origin()
def node_submit_job_user_constraint_cbk():
    para = flask.request.json

    manager.update_job_user_constraint(job_uid=para["job_uid"],
                                       user_constraint=para["user_constraint"])

    return flask.jsonify({
        "status": 0,
        "msg": "node updated constraint (manager.update_job_user_constraint)"
    })

# 云端内部接口：其他节点接入当前节点时，需要上传节点状态
@tracker_app.route("/node/update_status", methods=["POST"])
@flask_cors.cross_origin()
def node_update_status_cbk():
    para = flask.request.json
    root_logger.info("from {}: got {}".format(flask.request.remote_addr, para))
    node_ip = flask.request.remote_addr
    node_port = para['node_port']
    node_addr = node_ip + ":" + str(node_port)
    video_id = para['video_id']
    video_type = para['video_type']

    if node_addr not in node_status:
        node_status[node_addr] = dict()
    if "video" not in node_status[node_addr]:
        node_status[node_addr]["video"] = dict()
    if video_id not in node_status[node_addr]["video"]:
        node_status[node_addr]["video"][video_id] = dict()

    node_status[node_addr]["video"][video_id].update({"type": video_type})

    return flask.jsonify({"status": 0, "node_addr": node_addr})

# 工作节点内部接口：接受调度计划更新
@tracker_app.route("/node/update_plan", methods=["POST"])
@flask_cors.cross_origin()
def node_update_plan_cbk():
    para = flask.request.json
    root_logger.info("/node/update_plan got para={}".format(para))

    # 与工作节点模拟CPU执行的主循环竞争manager
    manager.update_job_plan(
        job_uid=para['job_uid'], video_conf=para['video_conf'], flow_mapping=para['flow_mapping'])

    return flask.jsonify({"status": 0, "msg": "node updated plan (manager.update_job_plan)"})

# 云端内部接口：接受调度请求
@tracker_app.route("/node/get_plan", methods=["POST"])
@flask_cors.cross_origin()
def node_get_plan_cbk():
    para = flask.request.json
    root_logger.info("/node/get_plan got para={}".format(para))

    manager.unsched_job_q.put(para)

    return flask.jsonify({"status": 0, "msg": "accepted (put to unsched_job_q)"})


def start_user_listener(serv_port=5000):
    user_app.run(host="0.0.0.0", port=serv_port)


def start_tracker_listener(serv_port=5001):
    tracker_app.run(host="0.0.0.0", port=serv_port)
    # app.run(port=serv_port)
    # app.run(host="*", port=serv_port)

# 云端调度器主循环：为manager的所有任务决定调度策略，并主动post策略到对应节点，让节点代理执行
# 不等待执行结果，节点代理执行完毕后post /user/系列接口提交结果
def cloud_scheduler_loop(manager=None):
    assert manager
    assert isinstance(manager, Manager)

    # import scheduler_func.demo_scheduler
    import scheduler_func.pid_scheduler

    while True:
        # 每5s调度一次
        time.sleep(5)

        try:
            # 获取资源情境
            r = manager.sess.get(
                url="http://{}/get_resource_info".format(manager.service_cloud_addr))
            resource_info = r.json()
            
            # 访问已注册的所有job实例，获取实例中保存的结果，生成调度策略
            for job in manager.job_dict:
                assert isinstance(job, Job)

                job_uid = job.get_job_uid()
                node_addr = manager.get_node_addr_by_job_uid(job_uid)
                last_plan_result = job.get_plan_result()
                user_constraint = job.get_user_constraint()
                assert node_addr

                conf, flow_mapping = scheduler_func.pid_scheduler.scheduler(
                    # flow=job.get_dag_flow(),
                    job_uid=job_uid,
                    dag={"generator": "x", "flow": job.get_pipeline()},
                    resource_info=resource_info,
                    last_plan_res=last_plan_result,
                    user_constraint=user_constraint
                )

                # 主动post策略到对应节点（即更新对应视频流query pipeline的执行策略），让节点代理执行，不等待执行结果
                r = manager.sess.post(url="http://{}/node/update_plan".format(node_addr),
                            json={"job_uid": job_uid, "video_conf": conf, "flow_mapping": flow_mapping})
        # except AssertionError as e:
        #     root_logger.error("caught assertion, msg={}".format(e), exc_info=True)
        except Exception as e:
            root_logger.error("caught exception, type={}, msg={}".format(
                repr(e), e), exc_info=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--side', dest='side', type=str, required=True)
    parser.add_argument('--mode', dest='mode', type=str, required=True)
    parser.add_argument('--cloud_ip', dest='cloud_ip',
                        type=str, default='127.0.0.1')
    parser.add_argument('--user_port', dest='user_port',
                        type=int, default=5000)
    parser.add_argument('--tracker_port', dest='tracker_port', 
                        type=int, default=5001)
    parser.add_argument('--pseudo_tracker_port',
                        dest='pseudo_tracker_port', type=int, default=5002)
    parser.add_argument('--serv_cloud_addr', dest='serv_cloud_addr',
                        type=str, default='127.0.0.1:5500')
    args = parser.parse_args()

    is_cloud = True if args.side == 'c' else False
    if is_cloud:
        # 云端Manager的背景线程：与用户通信，用户提交任务（5000端口）
        threading.Thread(target=start_user_listener,
                         args=(args.user_port,),
                         name="UserFlask",
                         daemon=True).start()

        # 云端Manager的背景线程：与工作节点Manager通信，下发job和调度策略（5001端口）
        threading.Thread(target=start_tracker_listener,
                         args=(args.tracker_port,),
                         name="TrackerFlask",
                         daemon=True).start()

        time.sleep(1)

    # 工作节点Manager的背景线程：与云端的Manager通信，同步job状态和调度策略（5001端口，伪分布式用5002）
    is_pseudo = True if args.mode == 'pseudo' else False
    if not is_cloud:
        assert not is_pseudo
        threading.Thread(target=start_tracker_listener,
                         args=(args.tracker_port,),
                         name="TrackerFlask",
                         daemon=True).start()
    elif is_pseudo:
        threading.Thread(target=start_tracker_listener,
                         args=(args.pseudo_tracker_port,),
                         name="TrackerFlask",
                         daemon=True).start()
    else:
        pass
        # assert False

    # 云端和工作节点设置云端node_addr
    manager.set_cloud_addr(cloud_ip=args.cloud_ip,
                           cloud_port=args.tracker_port)
    # 工作节点接入云端
    if not is_cloud:
        manager.join_cloud(local_port=args.tracker_port)
    elif is_pseudo:
        manager.join_cloud(local_port=args.pseudo_tracker_port)
    else:
        pass
        # assert False
    root_logger.info("joined to cloud")
    manager.set_service_cloud_addr(addr=args.serv_cloud_addr)

    # 云端的Scheduler线程/进程循环：从云端Manager获取未调度作业，计算调度策略，将策略下发工作节点Manager
    if is_cloud:
        if is_pseudo:
            threading.Thread(target=cloud_scheduler_loop,
                             args=(manager,),
                             name="CloudScheduler",
                             daemon=True).start()
        else:
            cloud_scheduler_loop(manager)

    # 工作节点Manager的执行线程循环
    # 一个Job对应一个视频流查询、对应一个进程/线程
    while (is_pseudo and is_cloud) or (not is_pseudo and not is_cloud):
        job = manager.start_new_job()

        sleep_sec = 5
        root_logger.warning(f"---- sleeping for {sleep_sec} sec ----")
        time.sleep(sleep_sec)
