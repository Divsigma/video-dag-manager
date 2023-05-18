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
    frame = cv2.resize(frame, [
        resolution_wh[video_conf['resolution']]['w'],
        resolution_wh[video_conf['resolution']]['h']
    ])

    input_ctx = dict()
    # input_ctx['image'] = (video_cap.get(cv2.CAP_PROP_POS_FRAMES), numpy.array(frame).shape)
    st_time = time.time()
    input_ctx['image'] = field_codec_utils.encode_image(frame)
    ed_time = time.time()
    root_logger.info("time consumed in encode-decode: {}".format(ed_time - st_time))
    # input_ctx['image'] = frame.tolist()

    root_logger.warning("only unsupport init task with one image frame as input")
    
    return input_ctx

def clpg_get_next_init_task(video_cap=None, video_conf=None):
    assert video_cap

    # 从视频流读取n帧（TODO：作为video_conf的调优参数，但其与时延关系不明朗）
    input_ctx = dict()
    input_ctx['clip'] = list()

    st_time = time.time()

    n = 5
    if 'ntracking' in video_conf.keys():
        n = video_conf['ntracking']

    for i in range(n):
        ret, frame = video_cap.read()
        assert ret
        
        # 根据video_conf['resolution']调整大小
        frame = cv2.resize(frame, [
            resolution_wh[video_conf['resolution']]['w'],
            resolution_wh[video_conf['resolution']]['h']
        ])
        
        input_ctx['clip'].append(field_codec_utils.encode_image(frame))

    ed_time = time.time()
    root_logger.info("time consumed in retriving-data: {}".format(ed_time - st_time))

    root_logger.warning("only unsupport init task with {} image frame as input".format(n))
    
    return input_ctx

class Generator():
    def __init__(self, video_id, video_url, gen_func):
        self.video_id = video_id
        self.cap = cv2.VideoCapture(video_url)
        self.gen_func = gen_func
        self.fpt = 1
    
    def get_current_clips(self):
        pass
    
    def get_fpt(self):
        return self.fpt

    def get_next_init_task(self, video_conf=None):
        self.fpt = 1
        if "ntracking" in video_conf.keys():
            self.fpt += video_conf['ntracking']
        input_ctx = self.gen_func(self.cap, video_conf)

        return input_ctx


class Manager():
    BEGIN_TASKNAME = "Start"
    BEGIN_TOPO_STEP = 0
    END_TASKNAME = "End"
    END_TOPO_STEP = 256
    # 设定生成器算子
    generator_func = {
        "SingleFrameGenerator": functools.partial(sfg_get_next_init_task),
        "ClipGenerator": functools.partial(clpg_get_next_init_task)
    }
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

    def get_cloud_addr(self):
        return self.cloud_addr
    
    def get_video_info_by_id(self, video_id=id):
        for info in self.video_info_list:
            if info["id"] == video_id:
                return info
        return None
    
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
    
    def get_chosen_service_url(self, taskname, choice):
        port = self.service_cloud_addr.split(':')[1]
        url = "http://{}:{}/execute_task/{}".format(choice["node_ip"], port, taskname)
        return url
    
    def join_cloud(self, local_port):
        # 接入云端，汇报自身信息
        for video_info in self.video_info_list:
            r = self.sess.post(url="http://{}/node/update_status".format(self.cloud_addr),
                              json={"node_port": local_port,
                                    "video_id": video_info["id"],
                                    "video_type": video_info["type"]})
            self.local_addr = r.json()["node_addr"]

    def generate_global_job_id(self):
        self.global_job_count += 1
        new_id = "GLOBAL_ID_" + str(self.global_job_count)
        return new_id
    
    # 云端/user/submit_job：争用node_status，修改node_addr的job_uid和node关系
    # 云端调度器需要根据job_uid找到节点，更新node_addr的任务调度策略：争用node_status，查询node_addr
    # 云端/user/submit_job_constraint需要根据job_uid找到节点，更新node_addr的任务约束：争用node_status，查询node_addr
    def get_node_addr_by_job_uid(self, job_uid):
        for node_addr, info in node_status.items():
            assert "job_uid_list" in info
            for uid in info["job_uid_list"]:
                if job_uid == uid:
                    return node_addr
        root_logger.error("cannot found job_uid-{} in node_status: {}".format(job_uid, node_status))
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
        
        root_logger.info("posted unsched_req for job-{} to cloud, got r={}".format(job.get_job_uid(), r.json()))
        

    # 工作节点更新调度计划：与通信进程竞争self.job_dict[job.get_job_uid()]，修改job状态
    def update_job_plan(self, job_uid, video_conf, flow_mapping):
        assert job_uid in self.job_dict.keys()

        job = self.job_dict[job_uid]
        assert isinstance(job, Job)
        job.set_plan(video_conf=video_conf, flow_mapping=flow_mapping)
        job.start_exec()

        root_logger.info("starting exec job-{}, updated plan".format(job.get_job_uid()))
    # 工作节点更新用户约束
    def update_job_user_constraint(self, job_uid, user_constraint):
        assert job_uid in self.job_dict.keys()

        job = self.job_dict[job_uid]
        assert isinstance(job, Job)

        job.set_user_constraint(user_constraint=user_constraint)
        root_logger.info("set job-{} user_constraint to '{}'".format(
            job.get_job_uid(),
            job.get_user_constraint()
        ))


    # 工作节点模拟CPU主循环的调度器
    def pop_one_exec_job(self):
        root_logger.info("job_dict keys: {}".format(self.job_dict.keys()))

        # 本地调度器：首先从可执行队列中调度获取所有可执行的job
        # while not self.exec_job_q.empty():
        #     new_exec_job = self.exec_job_q.get()
            
        #     assert new_exec_job.get_sched_state() == Job.JOB_STATE_UNSCHED
        #     assert isinstance(new_exec_job, Job)

        #     new_exec_job.start_exec()
        #     self.job_dict[new_exec_job.get_job_uid()] = new_exec_job


        # 云端调度器：与通信进程竞争self.job_dict[job.get_job_uid()]，修改job状态
        #            见update_job_plan函数

        # 遍历链表，选择一个可执行的job（参考linux0.12进程调度器）
        # 蓄水池算法
        sel_job = None
        n_executable_job = 0
        for job in self.job_dict.values():
            root_logger.info("job-{} status={}".format(job.get_job_uid(), job.get_sched_state()))
            if job.get_sched_state() == Job.JOB_STATE_EXEC:
                n_executable_job += 1
                if random.randint(1, n_executable_job) == 1:
                    sel_job = job
        
        if not sel_job:
            root_logger.warning("no job executable")
        else:
            assert isinstance(sel_job, Job)
            root_logger.info("schedule job-{} to exec".format(sel_job.get_job_uid()))

        return sel_job

    def submit_job(self, job_uid, dag_generator, dag_flow, dag_input, video_id):
        # 在本地启动新的job
        assert job_uid not in self.job_dict.keys()
        job = Job(job_uid=job_uid,
                  dag_generator=dag_generator, dag_flow=dag_flow, dag_input=dag_input,
                  video_id=video_id,
                  video_url=self.get_video_info_by_id(video_id)["url"],
                  generator_func=Manager.generator_func[dag_generator],
                  is_stream=True)
        job.set_manager(self)
        self.job_dict[job.get_job_uid()] = job
        self.post_reschedule_request(job)
        root_logger.info("current job_dict={}".format(self.job_dict.keys()))

        # 本地调度（未使用）：将新任务放入待调度队列
        # self.unsched_job_q.put(job)
        # 云端调度：

    def submit_job_result(self, job_uid, job_result, report2cloud=False):
        # 将Job本次提交的结果同步到本地
        if job_uid not in self.job_result_dict:
            self.job_result_dict[job_uid] = {"appended_result": list(), "latest_result": dict()}
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
            root_logger.warning("{} post /node/sync_job_result to itself".format(self.local_addr))

        if report2cloud:
            r = self.sess.post(url="http://{}/node/sync_job_result".format(self.cloud_addr),
                              json={"job_uid": job_uid,
                                    "job_result": job_result})
    
    def get_job_result(self, job_uid):
        if job_uid in self.job_result_dict:
            return self.job_result_dict[job_uid]
        return None
    
    def restart_job(self, job):
        # TODO：工作节点维护用户对任务的约束。
        assert isinstance(job, Job)
        should_reschedule = job.prepare_restart()

        if should_reschedule:
            # 云端调度
            self.post_reschedule_request(job)
            root_logger.info("prepare to reschedule job-{}, posted unsched_req to cloud".format(job.get_job_uid()))

            # 本地调度（未使用）
            # self.unsched_job_q.put(job)
            # root_logger.info("prepare to reschedule job-{}: put to unsched_job_q".format(job.get_job_uid()))
        
        root_logger.info("done job.prepare_restart(). RESTART job-{}".format(job.get_job_uid()))

    def remove_job(self, job):
        # 根据job的id移除job
        del self.job_dict[job.get_job_uid()]


class Job():
    JOB_STATE_UNSCHED = 0
    # JOB_STATE_SCHED = 1
    JOB_STATE_EXEC = 2
    JOB_STATE_DONE = 3

    def __init__(self, job_uid, dag_generator, dag_flow, dag_input, video_id, video_url, generator_func, is_stream):
        # job的全局唯一id
        self.job_uid = job_uid
        # DAG图信息
        self.dag_generator = dag_generator
        self.dag_flow = dag_flow
        self.dag_flow_input_deliminator = "."
        self.dag_input = dag_input
        self.loop_flag = is_stream
        # job的数据来源id及数据生成函数
        self.data_generator = Generator(video_id=video_id, video_url=video_url, gen_func=generator_func)
        # self.video_id = video_id
        # self.generator_func = generator_func
        # 调度状态机：执行计划与历史计划的执行结果
        self.flow_mapping = None
        self.video_conf = None
        self.plan_result = dict()
        self.user_constraint = None
        # 调度状态机：当前调度状态
        self.n_exec = 0
        self.added_plan_result = dict()
        self.sched_state = Job.JOB_STATE_UNSCHED
        # 执行状态机：当前所在的“拓扑步”
        self.topology_step = Manager.BEGIN_TOPO_STEP
        # 执行状态机：各步骤中间结果
        self.n_loop = 0
        self.task_result = dict()

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

    def get_job_uid(self):
        return self.job_uid
    
    def get_dag(self):
        return {"generator": self.dag_generator,
                "flow": self.dag_flow,
                "input": self.dag_input,
                "input_deliminator": self.dag_flow_input_deliminator}
    
    def get_dag_flow(self):
        return self.dag_flow
    
    def get_sched_state(self):
        return self.sched_state

    def get_loop_flag(self):
        return self.loop_flag
        
    # ---------------------------------------
    # ---- 执行计划与执行计划结果的相关函数 ----

    def set_plan(self, video_conf, flow_mapping):
        self.flow_mapping = flow_mapping
        self.video_conf = video_conf
        assert isinstance(self.flow_mapping, dict)
        assert isinstance(self.video_conf, dict)
    
    def get_plan(self):
        return {"video_conf": self.video_conf, "flow_mapping": self.flow_mapping}
    
    def get_plan_result(self):
        return self.plan_result
    
    def set_user_constraint(self, user_constraint):
        self.user_constraint = user_constraint
        assert isinstance(user_constraint, dict)
    
    def get_user_constraint(self):
        return self.user_constraint
    

    # -------------------------------
    # ---- Job最近一次执行后的结果 ----

    def get_latest_loop_result(self):
        taskname = self.prev_task_list[Manager.END_TASKNAME][0]
        result = None
        if taskname == 'car_detection':
            task_result = self.get_task_result(taskname=taskname, field="result")
            # result = {"n_loop": self.n_loop, "#cars": len(task_result)}
            result = {"n_loop": self.n_loop}
            result.update(task_result)
        if taskname == 'face_alignment':
            task_result = self.get_task_result(taskname=taskname, field="head_pose")
            result = {"n_loop": self.n_loop, "#headup": len(task_result)}
        if taskname == 'helmet_detection':
            task_result = self.get_task_result(taskname=taskname, field="clip_result")
            result = {"n_loop": self.n_loop, "#no_helmet": task_result[-1]['n_no_helmet']}

        return result
        # return self.get_task_result(taskname="SingleFrameGenerator",
        #                             field="image")

    def should_skip_loop(self):
        if self.video_conf and "nskip" in self.video_conf:
            nskip_conf = self.video_conf['nskip']
            if nskip_conf > 0 and (self.n_loop % nskip_conf != 0):
                return True
        return False

    def prepare_restart(self):
        # 执行状态机
        self.n_loop += 1

        # 根据已有的调度结果，决定下一帧是否处理。
        # 若处理，则清空所有结果，否则只清空generator的结果
        if not self.should_skip_loop():
            self.task_result = dict()
            root_logger.info("to handle loop: {}".format(self.n_loop))
        else:
            task_result_keys_copy = list(self.task_result.keys())
            for taskname in task_result_keys_copy:
                if taskname in Manager.generator_func.keys():
                    del self.task_result[taskname]
            root_logger.info("to skip loop: {}".format(self.n_loop))
        
        self.topology_step = Manager.BEGIN_TOPO_STEP

        # 调度状态机：重置时保留执行计划
        should_reschedule = False
        self.n_exec += 1
        if self.n_exec > 10:
            # 每10次执行，重新调度一次
            self.prepare_reschedule()
            should_reschedule = True
        
        return should_reschedule

    
    def start_exec(self):
        self.sched_state = Job.JOB_STATE_EXEC
    
    def prepare_reschedule(self):
        # 重置时保留执行计划，生成执行计划的统计结果

        assert self.n_exec > 0
        nframe_per_exec = self.data_generator.get_fpt()
        root_logger.info("n_exec={}, nframe_per_exec={}".format(self.n_exec, nframe_per_exec))
        for taskname, sum_delay in self.added_plan_result["delay"].items():
            root_logger.info("sum_delay of taskname({}): {}".format(taskname, sum_delay))
            self.added_plan_result["delay"][taskname] = sum_delay / (self.n_exec * nframe_per_exec * 1.0)

        self.plan_result = self.added_plan_result
        root_logger.info("calculated plan_result: {}".format(self.plan_result))

        # 重置调度状态
        # self.flow_mapping = None
        # self.video_conf = None
        self.n_exec = 0
        self.added_plan_result = dict()
        self.sched_state = Job.JOB_STATE_UNSCHED

    def set2done(self, msg):
        self.sched_state = Job.JOB_STATE_DONE
        root_logger.warning("job-{} is set to DONE with \nmsg: {}\n".format(
            self.get_job_uid(), msg
        ))

    def set_one_loop_to_end(self):
        self.topology_step = len(self.next_task_list) - 1
    def one_loop_is_end(self):
        return self.next_task_list[self.topology_step][0] == Manager.END_TASKNAME

    # -----------------------------------------
    # ---- 与Job的DAG中各个任务执行有关的函数 ----

    def is_generator_task(self, taskname):
        return taskname in Manager.generator_func.keys()

    def store_task_result(self, done_taskname, output_ctx):
        self.task_result[done_taskname] = output_ctx

    def get_task_result(self, taskname, field):
        # # 对数据生成的task，通过generator_func获取输入
        # # 注意需要实现为幂等：（1）判断数据是否已经读取（2）且Job重启时需要清空输入
        # if self.is_generator_task(taskname) and \
        #    taskname not in self.task_result.keys():
        #     # self.task_result[]
        #     self.task_result[taskname] = self.generator_func(self.manager, self.video_id)
        
        # 对其他task，直接获取Job对象中缓存的中间结果
        root_logger.info("to get task({}) result".format(taskname))
        assert taskname in self.task_result.keys()
        root_logger.info("taskname({}) task_res keys: {}".format(taskname, self.task_result[taskname].keys()))
        assert field in self.task_result[taskname].keys()

        return self.task_result[taskname][field]

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
        
        st_time = time.time()
        r = self.sess.post(url=serv_url, json=input_ctx)
        ed_time = time.time()

        try:
            res = r.json()
            root_logger.info("got service result: {}, (delta_t={})".format(res.keys(), ed_time - st_time))
            # 记录任务的执行结果
            self.store_task_result(taskname, r.json())
            # 累计执行计划的执行结果
            if "delay" not in self.added_plan_result.keys():
                self.added_plan_result["delay"] = dict()
            if taskname not in self.added_plan_result["delay"].keys():
                self.added_plan_result["delay"][taskname] = 0
            self.added_plan_result["delay"][taskname] += ed_time - st_time
            root_logger.info("update added_plan_result: {}".format(self.added_plan_result))

            return True

        except Exception as e:
            root_logger.error("caught exception: {}".format(e), exc_info=True)
            root_logger.error("got serv result: {}".format(r.text))
            return False

        return False
    

    # ---------------------------------------------------------------------
    # ---- 确定执行计划的Job被CPU调度后，按计划执行任务的主函数（非抢占式） ----

    def forward_one_step(self):
        # TODO：将Job推进一步或根据跳帧率处理
        nt_list = self.next_task_list[self.topology_step]
        root_logger.info("got job next_task_list - {}".format(nt_list))

        # 若跳帧，则仅读取数据但不处理
        # if self.should_skip_loop() and self.topology_step == Manager.BEGIN_TOPO_STEP:
        #     root_logger.info("skipping n_loop {}".format(self.n_loop))
        #     self.get_task_input(nt_list[0])
        #     self.set_one_loop_to_end()
        #     return

        # 若跳帧，则仅读取数据但不处理
        if self.should_skip_loop():
            # 逻辑assertion：一个循环执行过程中不会出现self.should_skip_loop()==True情况
            root_logger.info("skipping n_loop {}".format(self.n_loop))
            taskname = nt_list[0]
            assert self.is_generator_task(taskname)
            output_ctx = self.data_generator.get_next_init_task(video_conf=self.video_conf)
            self.store_task_result(taskname, output_ctx=output_ctx)
            # 终止本次 DAG执行循环
            self.set_one_loop_to_end()
            return


        available_service_list = self.manager.get_available_service_list()
        root_logger.info("got available_service_list: {}".format(available_service_list))

        for taskname in nt_list:
            root_logger.info("to forward taskname={}".format(taskname))

            # 对Generator任务，读取数据后返回
            if self.is_generator_task(taskname):
                # 根据video_conf，获取当前任务的输入数据
                output_ctx = self.data_generator.get_next_init_task(video_conf=self.video_conf)
                self.store_task_result(taskname, output_ctx=output_ctx)
                root_logger.info("done generator task, get_next_init_task({})".format(output_ctx.keys()))
                continue

            # 对其他可调用任务，获取输入数据，并调用url
            assert taskname in available_service_list

            input_ctx = self.get_task_input(taskname)
            root_logger.info("get input_ctx({}) of taskname({})".format(
                input_ctx.keys(),
                taskname
            ))

            # 根据flow_mapping，执行task，并记录中间结果
            root_logger.info("flow_mapping ={}".format(self.flow_mapping))            
            choice = self.flow_mapping[taskname]
            # execute_url_dict = self.manager.get_service_dict(taskname)
            # root_logger.info("get execute_url_dict {}".format(execute_url_dict))
            root_logger.info("get choice of '{}' in flow_mapping, choose: {}".format(taskname, choice))
            url = self.manager.get_chosen_service_url(taskname, choice)
            root_logger.info("get url {}".format(url))

            self.invoke_service(serv_url=url, taskname=taskname, input_ctx=input_ctx)
        
        self.topology_step += 1








# 单例变量：主线程任务管理器，Manager
manager = Manager()
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
@user_app.route("/user/submit_job", methods=["POST"])
@flask_cors.cross_origin()
def user_submit_job_cbk():
    # 获取用户针对视频流提交的job，转发到对应边端
    para = flask.request.json
    root_logger.info("/user/submit_job got para={}".format(para))
    node_addr = para['node_addr']
    video_id = para['video_id']

    if node_addr not in node_status:
        return flask.jsonify({"status": 1, "error": "cannot found {}".format(node_addr)})
    
    # TODO：切分DAG产生多个SUB_ID
    unique_job_id = manager.generate_global_job_id() + "." + "SUB_ID"

    # TODO：维护job_uid和节点关系
    if "job_uid_list" not in node_status[node_addr]:
        node_status[node_addr]["job_uid_list"] = list()
    node_status[node_addr]["job_uid_list"].append(unique_job_id)

    new_req_para = dict()
    new_req_para["unique_job_id"] = unique_job_id
    new_req_para.update(para)
    r = manager.sess.post(url="http://{}/node/submit_job".format(node_addr),
                      json=new_req_para)
    # r = requests.post(url="http://{}/node/submit_job".format(node_addr),
    #                   json=new_req_para)

    if r.ok:
        root_logger.info("got ret: {}".format(r.json()))
        return flask.jsonify(r.json())

    return flask.jsonify(r.text)

# 外部接口：从云端获取用户对job的约束，需要传入job_uid
@user_app.route("/user/submit_job_user_constraint", methods=["POST"])
@flask_cors.cross_origin()
def user_submit_job_user_constraint_cbk():
    para = flask.request.json
    root_logger.info("/user/submit_job_user_constraint got para={}".format(para))
    
    # 后端校验？否则调度器可能抛出“无法比较str和float”
    assert ("job_uid" in para) and ("user_constraint" in para)
    assert ("delay" in para["user_constraint"])
    assert (isinstance(para["user_constraint"]["delay"], int) or \
            isinstance(para["user_constraint"]["delay"], float))

    job_uid = para["job_uid"]
    node_addr = manager.get_node_addr_by_job_uid(job_uid)
    assert node_addr

    new_req_para = dict()
    new_req_para["job_uid"] = job_uid
    new_req_para["user_constraint"] = para["user_constraint"]
    r = manager.sess.post(url="http://{}/node/submit_job_user_constraint".format(node_addr),
                          json=new_req_para)
    
    if r.ok:
        # 提交成功则转发响应
        root_logger.info("got ret: {}".format(r.json()))
        return flask.jsonify(r.json())
    
    return flask.jsonify({"msg": "request to /node not ok", "text": r.text})

# 外部接口：从云端获取job执行结果，需要传入job_uid
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
    root_logger.info("got {}".format(para))
    generator_name = para["dag"]["generator"]
    if generator_name not in Manager.generator_func.keys():
        return flask.jsonify({"status": 1, "msg": "unsupport generator name"})
    manager.submit_job(job_uid=para["unique_job_id"],
                       dag_generator=para["dag"]["generator"],
                       dag_flow=para["dag"]["flow"],
                       dag_input=para["dag"]["input"],
                       video_id=para["video_id"])
    return flask.jsonify({"status": 0,
                          "msg": "submitted to manager from api: node/submit_job",
                          "job_uid": para["unique_job_id"]})

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
    manager.update_job_plan(job_uid=para['job_uid'], video_conf=para['video_conf'], flow_mapping=para['flow_mapping'])

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

# 云端调度器主循环：从unsched_job_q中取一未调度的任务请求，生成调度计划
def cloud_scheduler_loop(manager=None):
    assert manager
    assert isinstance(manager, Manager)

    unsched_job_q = manager.unsched_job_q
    exec_job_q = manager.exec_job_q
    assert unsched_job_q
    assert exec_job_q

    sess = requests.Session()

    import scheduler_func.demo_scheduler

    while True:
        try:
            sched_req = unsched_job_q.get()
            assert isinstance(sched_req, dict)
            root_logger.info("got one sched_req from unsched_job_q: {}".format(sched_req))

            job_uid = sched_req['job_uid']
            node_addr = manager.get_node_addr_by_job_uid(job_uid)
            assert node_addr

            r = sess.get(url="http://{}/get_resource_info".format(manager.service_cloud_addr))
            conf, flow_mapping = scheduler_func.demo_scheduler.scheduler(
                # flow=job.get_dag_flow(),
                job_uid=job_uid,
                dag=sched_req['dag'],
                resource_info=r.json(),
                last_plan_res=sched_req['last_plan_result'],
                user_constraint=sched_req['user_constraint']
            )

            r = sess.post(url="http://{}/node/update_plan".format(node_addr),
                          json={"job_uid": job_uid, "video_conf": conf, "flow_mapping": flow_mapping})
        # except AssertionError as e:
        #     root_logger.error("caught assertion, msg={}".format(e), exc_info=True)
        except Exception as e:
            root_logger.error("caught exception, type={}, msg={}".format(repr(e), e), exc_info=True)
        

# 本地调度器主循环（未使用）：从unsched_job_q中取一未调度任务，生成调度计划，修改Job状态，放入待执行队列
def local_scheduler_loop(unsched_job_q=None, exec_job_q=None, serv_cloud_addr="127.0.0.1:5500"):
    
    assert unsched_job_q
    assert exec_job_q

    sess = requests.Session()

    import scheduler_func.demo_scheduler

    while True:
        try:
            job = unsched_job_q.get()
            assert isinstance(job, Job)

            r = sess.get(url="http://{}/get_resource_info".format(serv_cloud_addr))
            last_plan_result = job.get_plan_result()
            last_plan_result = None if not bool(last_plan_result) else last_plan_result
            conf, flow_mapping = scheduler_func.demo_scheduler.scheduler(
                # flow=job.get_dag_flow(),
                dag=job.get_dag(),
                resource_info=r.json(),
                last_plan_res=last_plan_result,
                user_constraint={ "delay": [0, 0.3],  "acc_level": 5 }
            )
            job.set_plan(video_conf=conf, flow_mapping=flow_mapping)

            exec_job_q.put(job)

        except Exception as e:
            root_logger.error("caught exception: {}".format(e))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--side', dest='side', type=str, required=True)
    parser.add_argument('--mode', dest='mode', type=str, required=True)
    parser.add_argument('--cloud_ip', dest='cloud_ip', type=str, default='127.0.0.1')
    parser.add_argument('--user_port', dest='user_port', type=int, default=5000)
    parser.add_argument('--tracker_port', dest='tracker_port', type=int, default=5001)
    parser.add_argument('--pseudo_tracker_port', dest='pseudo_tracker_port', type=int, default=5002)
    parser.add_argument('--serv_cloud_addr', dest='serv_cloud_addr', type=str, default='127.0.0.1:5500')
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
        assert False

    # 工作节点接入云端
    manager.set_cloud_addr(cloud_ip=args.cloud_ip, cloud_port=args.tracker_port)
    if not is_cloud:
        manager.join_cloud(local_port=args.tracker_port)
    elif is_pseudo:
        manager.join_cloud(local_port=args.pseudo_tracker_port)
    else:
        assert False
    root_logger.info("joined to cloud")
    manager.set_service_cloud_addr(addr=args.serv_cloud_addr)

    # 云端的Scheduler线程/进程：从云端Manager获取未调度作业，计算调度策略，将策略下发工作节点Manager
    if is_cloud:
        unsched_job_q = queue.Queue(10)
        exec_job_q= queue.Queue(10)
        manager.set_unsched_job_q(unsched_job_q)
        manager.set_exec_job_q(exec_job_q)
        if is_pseudo:
            threading.Thread(target=cloud_scheduler_loop,
                            args=(manager,),
                            name="CloudScheduler",
                            daemon=True).start()
        else:
            cloud_scheduler_loop(manager)
        # unsched_job_q = mp.Queue(10)
        # exec_job_q= mp.Queue(10)
        # manager.set_unsched_job_q(unsched_job_q)
        # manager.set_exec_job_q(exec_job_q)
        # mp.Process(target=scheduler_loop,
        #            args=(unsched_job_q, exec_job_q, args.serv_cloud_addr)).start()

    # 工作节点Manager的执行线程（一个线程模拟一个CPU核）：
    # 一个确定了执行计划的Job相当于一个进程
    # 非抢占式选取job执行，每次选取后执行一个“拓扑步”
    while (is_pseudo and is_cloud) or (not is_pseudo and not is_cloud):
        job = manager.pop_one_exec_job()

        if job is None:
            sleep_sec = 4
            root_logger.warning(f"---- no job, sleeping for {sleep_sec} sec ----")
            time.sleep(sleep_sec)
            continue

        assert isinstance(job, Job)
        root_logger.info("got job - {}".format(job))
        
        try:
            job.forward_one_step()
        except Exception as e:
            root_logger.error("caught exception, type={}, msg={}".format(repr(e), e), exc_info=True)
            root_logger.error("set2done and removing job: {}".format(job))
            job.set2done(msg=[repr(e), e])
            manager.remove_job(job)
        
        if job.one_loop_is_end():
            # 当前job完成后，立刻汇报结果
            manager.submit_job_result(job_uid=job.get_job_uid(),
                                      job_result={
                                          "appended_result": job.get_latest_loop_result(),
                                          "latest_result": {
                                                "plan": job.get_plan(),
                                                "plan_result": job.get_plan_result()   
                                          }
                                      },
                                      report2cloud=not is_cloud)
            
            # 若当前job未完成对应数据流的处理，则重启当前job
            # NOTES：job的generator_func需要维护状态机，持续生成数据
            if job.get_loop_flag():
                root_logger.info("restart job: {}".format(job))
                manager.restart_job(job)
            else:
                root_logger.info("remove job: {}".format(job))
                manager.remove_job(job)

        # time.sleep(1)



