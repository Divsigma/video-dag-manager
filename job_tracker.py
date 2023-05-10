import cv2
import numpy
import flask
import flask_cors
import random
import requests
import threading
import queue
import time
import functools
import argparse
from werkzeug.serving import WSGIRequestHandler

import field_codec_utils
from logging_utils import root_logger

# SingleFrameGenerator的数据生成函数
def init_task_wrapper(manager, video_id):
    # # 模拟产生数据
    # frame = list()
    # for i in range(10):
    #     frame.append(list())
    #     for j in range(3):
    #         frame[i].append(random.randint(1,1000))
    # import numpy
    # frame = numpy.array(frame)

    # 从视频流读取一帧
    video_cap_dict = manager.get_video_cap_dict()
    assert video_id in video_cap_dict.keys()

    video_cap = video_cap_dict[video_id]
    
    ret, frame = video_cap.read()
    assert ret

    input_ctx = dict()
    # input_ctx['image'] = (video_cap.get(cv2.CAP_PROP_POS_FRAMES), numpy.array(frame).shape)
    # input_ctx['image'] = field_codec_utils.encode_image(frame)
    input_ctx['image'] = frame.tolist()

    root_logger.warning("only unsupport init task with one image frame as input")
    
    return input_ctx

class Manager():
    BEGIN_TASKNAME = "Start"
    BEGIN_TOPO_STEP = 0
    END_TASKNAME = "End"
    END_TOPO_STEP = 256
    generator_func = {
        "SingleFrameGenerator": functools.partial(init_task_wrapper)
    }

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
            {"id": 0, "type": "student in classroom"},
            {"id": 1, "type": "people in meeting-room"},
            # {"id": 3, "type": "traffic flow outdoor"}
        ]
        self.video_cap_dict = {
            0: cv2.VideoCapture("input.mov"),
            1: cv2.VideoCapture("input1.mp4"),
            # 3: cv2.VideoCapture("traffic-720p.mp4")
        }

        # 模拟数据库：记录下发到本地的job以及该job的执行结果
        self.global_job_count = 0
        self.job_dict = dict()
        self.job_result_dict = dict()

        # 调度队列
        self.unsched_job_q = None
        # 执行队列
        self.exec_job_q = None

    def set_unsched_job_q(self, q):
        self.unsched_job_q = q

    def set_exec_job_q(self, q):
        self.exec_job_q = q

    def set_cloud_addr(self, cloud_ip, cloud_port):
        self.cloud_addr = cloud_ip + ":" + str(cloud_port)

    def set_service_cloud_addr(self, addr):
        self.service_cloud_addr = addr

    def get_video_cap_dict(self):
        return self.video_cap_dict

    def get_cloud_addr(self):
        return self.cloud_addr
    
    
    def get_available_service_list(self):
        r = self.sess.get(url="http://{}/get_service_list".format(self.service_cloud_addr))
        assert isinstance(r.json(), list)
        return r.json()

    def get_service_dict(self, taskname):
        r = self.sess.get(url="http://{}/get_execute_url/{}".format(
            self.service_cloud_addr,
            taskname
        ))
        assert isinstance(r.json(), dict)
        return r.json()
    
    def join_cloud(self, local_port):
        # 接入云端，汇报自身信息
        for video_info in self.video_info_list:
            r = self.sess.post(url="http://{}/node/update_status".format(self.cloud_addr),
                              json={"node_port": local_port,
                                    "video_id": video_info["id"],
                                    "video_type": video_info["type"]})
            # r = requests.post(url="http://{}/node/update_status".format(self.cloud_addr),
            #                   json={"node_port": local_port,
            #                         "video_id": video_info["id"],
            #                         "video_type": video_info["type"]})
            self.local_addr = r.json()["node_addr"]

    def generate_global_job_id(self):
        self.global_job_count += 1
        new_id = "GLOBAL_ID_" + str(self.global_job_count)
        return new_id

    def pop_one_exec_job(self):
        root_logger.info("job_dict keys: {}".format(self.job_dict.keys()))

        # 首先从可执行队列中调度获取所有可执行的job
        while not self.exec_job_q.empty():
            new_exec_job = self.exec_job_q.get()
            
            assert new_exec_job.get_sched_state() == Job.JOB_STATE_UNSCHED
            assert isinstance(new_exec_job, Job)

            new_exec_job.start_exec()
            self.job_dict[new_exec_job.get_job_uid()] = new_exec_job

        # 遍历链表，选择一个可执行的job（参考linux0.12进程调度器）
        sel_job = None
        for job in self.job_dict.values():
            if job.get_sched_state() == Job.JOB_STATE_EXEC:
                sel_job = job
                root_logger.info("schedule job-{} to exec".format(job.get_job_uid()))

        root_logger.warning("no job executable")
        return sel_job

    def submit_job(self, job_uid, dag_flow, dag_input, video_id, generator_func_name):
        # 在本地启动新的job
        assert job_uid not in self.job_dict.keys()
        job = Job(job_uid=job_uid,
                  dag_flow=dag_flow, dag_input=dag_input,
                  video_id=video_id,
                  generator_func=Manager.generator_func[generator_func_name],
                  is_stream=True)
        job.set_manager(self)
        self.job_dict[job.get_job_uid()] = job
        # 将新任务放入待调度队列
        self.unsched_job_q.put(job)

    def submit_job_result(self, job_uid, job_result, report2cloud=False):
        # 将结果保存到本地
        if job_uid not in self.job_result_dict:
            self.job_result_dict[job_uid] = list()
        self.job_result_dict[job_uid].append(job_result)

        # 将结果同步到云端（注意/node/sync_job_result的处理，要避免死循环）
        if self.cloud_addr == self.local_addr:
            root_logger.warning("{} post /node/sync_job_result to itself".format(self.local_addr))

        if report2cloud:
            r = self.sess.post(url="http://{}/node/sync_job_result".format(self.cloud_addr),
                              json={"job_uid": job_uid,
                                    "job_result": job_result})
            # r = requests.post(url="http://{}/node/sync_job_result".format(self.cloud_addr),
            #                   json={"job_uid": job_uid,
            #                         "job_result": job_result})
    
    def get_job_result(self, job_uid):
        if job_uid in self.job_result_dict:
            return self.job_result_dict[job_uid]
        return None

    def reschedule_job(self, job):
        job.restart()
        self.unsched_job_q.put(job)
        root_logger.info("RESTART job-{}: put to unsched_job_q".format(job.get_job_uid()))

    def remove_job(self, job):
        # 根据job的id移除job
        del self.job_dict[job.get_job_uid()]


class Job():
    JOB_STATE_UNSCHED = 0
    # JOB_STATE_SCHED = 1
    JOB_STATE_EXEC = 2
    JOB_STATE_DONE = 3

    def __init__(self, job_uid, dag_flow, dag_input, video_id, generator_func, is_stream):
        # job的全局唯一id
        self.job_uid = job_uid
        # DAG图信息
        self.dag_flow = dag_flow
        self.dag_flow_input_deliminator = "."
        self.dag_input = dag_input
        self.loop_flag = is_stream
        # job的数据来源id及数据生成函数
        self.video_id = video_id
        self.generator_func = generator_func
        # 调度状态机：执行计划
        self.exec_plan = None
        self.flow_mapping = None
        self.video_conf = None
        # 调度状态机：当前调度状态
        self.sched_state = Job.JOB_STATE_UNSCHED
        # 执行状态机：当前所在的“拓扑步”
        self.topology_step = Manager.BEGIN_TOPO_STEP
        # 执行状态机：各步骤中间结果
        self.res = dict()

        self.manager = None
        # keepalive的http客户端
        self.sess = requests.Session()

        # 拓扑解析dag图
        # NOTES: 目前仅支持流水线
        #        Start -> D -> C -> End
        #          0      1    2     3
        self.next_task_list = dict()
        self.prev_task_list = dict()

        assert isinstance(self.dag_flow, list)

        prev_taskname = Manager.BEGIN_TASKNAME
        curr_step = Manager.BEGIN_TOPO_STEP
        for name in self.dag_flow:
            self.next_task_list[curr_step] = [ name ]
            self.prev_task_list[name] = [ prev_taskname ]
            curr_step += 1
            prev_taskname = name
        self.next_task_list[curr_step] = [ Manager.END_TASKNAME ]
        self.prev_task_list[ Manager.END_TASKNAME ] = [ self.dag_flow[-1] ]

    def set_manager(self, manager):
        self.manager = manager
        assert isinstance(self.manager, Manager)

    def set_exec_plan(self, plan):
        self.exec_plan = plan
        self.flow_mapping = plan['flow_mapping']
        self.video_conf = plan['video_conf']
        assert isinstance(self.flow_mapping, dict)
        assert isinstance(self.video_conf, dict)

    def get_job_uid(self):
        return self.job_uid
    
    def get_sched_state(self):
        return self.sched_state

    def get_loop_flag(self):
        return self.loop_flag

    def get_job_result(self):
        taskname = self.prev_task_list[Manager.END_TASKNAME][0]
        result = None
        if taskname == 'car_detection':
            result = self.get_task_result(taskname=taskname, field="result")
        if taskname == 'face_alignment':
            result = self.get_task_result(taskname=taskname, field="head_pose")

        if result:
            return len(result)
        return None
        # return self.get_task_result(taskname="SingleFrameGenerator",
        #                             field="image")

    def restart(self):
        # 调度状态机
        self.exec_plan = None
        self.flow_mapping = None
        self.video_conf = None
        self.sched_state = Job.JOB_STATE_UNSCHED
        # 执行状态机
        self.res = dict()
        self.topology_step = Manager.BEGIN_TOPO_STEP
    
    def start_exec(self):
        self.sched_state = Job.JOB_STATE_EXEC

    def set2done(self, msg):
        self.sched_state = Job.JOB_STATE_DONE
        root_logger.warning("job-{} is set to DONE with \nmsg: {}\n".format(
            self.get_job_uid(), msg
        ))

    def end_one_loop(self):
        return self.next_task_list[self.topology_step][0] == Manager.END_TASKNAME

    def store_task_result(self, done_taskname, output_ctx):
        self.res[done_taskname] = output_ctx

    def get_task_result(self, taskname, field):
        # 对数据生成的task，通过generator_func获取输入（需要实现为幂等）
        if taskname in Manager.generator_func.keys() and taskname not in self.res.keys():
            self.res[taskname] = self.generator_func(self.manager, self.video_id)
        
        # 对其他task，直接获取Job对象中缓存的中间结果
        assert taskname in self.res.keys()
        root_logger.info("task res keys: {}".format(self.res[taskname].keys()))
        assert field in self.res[taskname].keys()

        return self.res[taskname][field]

    def get_task_input(self, curr_taskname):
        ctx = dict()
        # 根据当前任务，寻找依赖任务
        # 将依赖任务的结果放进ctx返回
        for k, v in self.dag_input[curr_taskname].items():
            prev_taskname = v.split(self.dag_flow_input_deliminator)[0]
            prev_field = v.split(self.dag_flow_input_deliminator)[1]
            ctx[k] = self.get_task_result(taskname=prev_taskname, field=prev_field)
        return ctx
    
    def invoke_service(self, serv_url, taskname, input_ctx):
        root_logger.info("get serv_url={}".format(serv_url))
        
        r = self.sess.post(url=serv_url, json=input_ctx)

        try:
            res = r.json()
            root_logger.info("got service result: {}".format(res.keys()))
            self.store_task_result(taskname, r.json())
            return True

        except Exception as e:
            root_logger.error("caught exception: {}".format(e))
            root_logger.error("got serv result: {}".format(r.text))
            return False

        return False
    
    def forward_one_step(self):
        # 将Job推进一步
        # TODO: 根据预测情况，选择task执行的节点
        nt_list = self.next_task_list[self.topology_step]
        root_logger.info("got job next_task_list - {}".format(nt_list))

        available_service_list = self.manager.get_available_service_list()

        for taskname in nt_list:
            assert taskname in available_service_list
            execute_url_dict = self.manager.get_service_dict(taskname)

            # 根据video_conf，获取当前任务的输入数据
            input_ctx = self.get_task_input(taskname)
            root_logger.info("get input_ctx({}) of taskname({})".format(
                input_ctx.keys(),
                taskname
            ))

            # 根据flow_mapping，执行task
            choice = self.flow_mapping[taskname]
            root_logger.info("get execute_url_dict {}".format(execute_url_dict))
            root_logger.info("get flow_mapping of '{}': {}".format(taskname, choice))
            url = list(execute_url_dict.values())[choice['node_id']]['url']
            root_logger.info("get url {}".format(url))

            self.invoke_service(serv_url=url, taskname=taskname, input_ctx=input_ctx)
        
        self.topology_step += 1




# 单例变量：主线程任务管理器，Manager
manager = Manager()
# 单例变量：后台web线程
flask.Flask.logger_name = "listlogger"
WSGIRequestHandler.protocol_version = "HTTP/1.1"
app = flask.Flask(__name__)
flask_cors.CORS(app)

# 模拟数据库
# 单例变量：接入到当前节点的节点信息
node_status = dict()




# 外部接口：从云端接受用户提交的job参数，包括指定的DAG、数据来源，将/node/submit_job的注册结果返回
@app.route("/user/submit_job", methods=["POST"])
@flask_cors.cross_origin()
def user_submit_job_cbk():
    # 获取用户针对视频流提交的job，转发到对应边端
    para = flask.request.json
    root_logger.info("{}".format(para))
    node_addr = para['node_addr']
    video_id = para['video_id']

    if node_addr not in node_status:
        return flask.jsonify({"status": 1, "error": "cannot found {}".format(node_addr)})
    
    # TODO：切分DAG产生多个SUB_ID
    new_req_para = dict()
    new_req_para["unique_job_id"] = manager.generate_global_job_id() + "." + "SUB_ID"
    new_req_para.update(para)
    r = manager.sess.post(url="http://{}/node/submit_job".format(node_addr),
                      json=new_req_para)
    # r = requests.post(url="http://{}/node/submit_job".format(node_addr),
    #                   json=new_req_para)

    if r.ok:
        root_logger.info("got ret: {}".format(r.json()))
        return flask.jsonify(r.json())

    return flask.jsonify(r.text)

# 外部接口：从云端获取job执行结果，需要传入job_uid
@app.route("/user/sync_job_result/<job_uid>", methods=["GET"])
@flask_cors.cross_origin()
def user_sync_job_result_cbk(job_uid):
    job_result = manager.get_job_result(job_uid=job_uid)
    return flask.jsonify({"status": 0,
                          "result": job_result})

# 内部接口：节点间同步job的执行结果到本地
@app.route("/node/sync_job_result", methods=["POST"])
@flask_cors.cross_origin()
def node_sync_job_result_cbk():
    para = flask.request.json

    # NOTES：防止再次请求/node/sync_job_result接口，否则死锁
    manager.submit_job_result(job_uid=para["job_uid"],
                              job_result=para["job_result"],
                              report2cloud=False)
    
    return flask.jsonify({"status": 200})

# 内部接口：接受其他节点传入的job初始化参数，在本地生成可以执行的job
@app.route("/node/submit_job", methods=["POST"])
@flask_cors.cross_origin()
def node_submit_job_cbk():
    # 获取产生job的初始化参数
    para = flask.request.json
    root_logger.info("got {}".format(para))
    generator_func_name = para["generator"]
    if generator_func_name not in Manager.generator_func:
        return flask.jsonify({"status": 1, "msg": "unsupport generator func name"})
    manager.submit_job(job_uid=para["unique_job_id"],
                       dag_flow=para["dag"]["flow"],
                       dag_input=para["dag"]["input"],
                       video_id=para["video_id"],
                       generator_func_name=generator_func_name)
    return flask.jsonify({"status": 0,
                          "msg": "submitted to manager from api: node/submit_job",
                          "job_uid": para["unique_job_id"]})

# 内部接口：其他节点接入当前节点时，需要上传节点状态
@app.route("/node/update_status", methods=["POST"])
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

# 内部接口：获取当前节点所知的所有节点信息
@app.route("/node/get_all_status")
@flask_cors.cross_origin()
def node_get_all_status_cbk():
    return flask.jsonify({"status": 0, "data": node_status})


def start_dag_listener(serv_port=5000):
    app.run(host="0.0.0.0", port=serv_port)
    # app.run(port=serv_port)
    # app.run(host="*", port=serv_port)


# 调度器函数：可选模块
def scheduler_func(dag=None, generator_output=None, resource_info=None, last_plan_res=None):
    video_conf = {
        "resolution": "480p",
        "fps": 30,
        "encoder": "H264",
    }
    flow_mapping = {
        "face_detection": {
            "model_id": 0,
            "node_id": 0,
        },
        "face_alignment": {
            "model_id": 0,
            "node_id": 1,
        }
    }
    return {"video_conf": video_conf, "flow_mapping": flow_mapping}

# 调度器主循环：从unsched_job_q中取一未调度任务，生成调度计划，修改Job状态，放入待执行队列
def scheduler_loop(unsched_job_q=None, exec_job_q=None):
    
    assert unsched_job_q
    assert exec_job_q

    while True:
        job = unsched_job_q.get()
        
        plan = scheduler_func()
        job.set_exec_plan(plan)

        exec_job_q.put(job)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--side', dest='side', type=str, required=True)
    parser.add_argument('--cloud_ip', dest='cloud_ip', type=str, default='127.0.0.1')
    parser.add_argument('--cloud_port', dest='cloud_port', type=int, default=5000)
    parser.add_argument('--local_port', dest='local_port', type=int, default=5001)
    parser.add_argument('--serv_cloud_addr', dest='serv_cloud_addr', type=str, default='127.0.0.1:5500')
    args = parser.parse_args()

    # 云端Manager的背景线程：接收节点接入、用户提交任务
    if args.side == 'c':
        threading.Thread(target=start_dag_listener,
                        args=(args.cloud_port,),
                        daemon=True).start()
        time.sleep(1)

    # 工作节点Manager的背景线程：接收云端下发的job
    threading.Thread(target=start_dag_listener,
                     args=(args.local_port,),
                     daemon=True).start()

    manager.set_cloud_addr(cloud_ip=args.cloud_ip, cloud_port=args.cloud_port)
    manager.join_cloud(local_port=args.local_port)
    manager.set_service_cloud_addr(addr=args.serv_cloud_addr)
    # if args.side == 'e':
    #     manager.join_cloud(local_port=args.local_port)

    # 工作节点的Scheduler线程/进程：
    # 与Manager通信，从Manager获取未调度作业，往Manager提交待执行作业
    unsched_job_q = queue.Queue(10)
    exec_job_q= queue.Queue(10)
    manager.set_unsched_job_q(unsched_job_q)
    manager.set_exec_job_q(exec_job_q)
    threading.Thread(target=scheduler_loop,
                     args=(unsched_job_q, exec_job_q),
                     daemon=True).start()

    # 工作节点Manager的执行线程（一个线程模拟一个CPU核）：
    # 一个确定了执行计划的Job相当于一个进程
    # 非抢占式选取job执行，每次选取后执行一个“拓扑步”
    while True:
        job = manager.pop_one_exec_job()
        if job is None:
            root_logger.warning("no job, sleep for 4 sec")
            time.sleep(4)
            continue

        root_logger.info("got job - {}".format(job))
        
        try:
            job.forward_one_step()
        except Exception as e:
            job.set2done(msg=e)
            root_logger.error("caught exception: {}".format(e))
            root_logger.warning("remove job: {}".format(job))
            manager.remove_job(job)
        
        if job.end_one_loop():
            # 当前job完成后，立刻汇报结果
            manager.submit_job_result(job_uid=job.get_job_uid(),
                                      job_result=job.get_job_result(),
                                      report2cloud=True)
            
            # 若当前job未完成对应数据流的处理，则重启当前job
            # NOTES：job的generator_func需要维护状态机，持续生成数据
            if job.get_loop_flag():
                root_logger.info("restart job: {}".format(job))
                manager.reschedule_job(job)
            else:
                root_logger.info("remove job: {}".format(job))
                manager.remove_job(job)

        # time.sleep(1)



