from logging_utils import root_logger
import pandas as pd
import os

import time

prev_video_conf = dict()

prev_flow_mapping = dict()

prev_runtime_info = dict()

available_fps = [1, 5, 10, 20, 30]
available_resolution = ["360p", "480p", "720p", "1080p"]
# available_npxpf = [480*360, 858*480, 1280*720, 1920*1080]

lastTime = time.time()


class PIDController:
    def __init__(self, Kp, Ki, Kd, setpoint, dt):
        self.Kp = Kp
        self.Ki = Ki
        self.Kd = Kd
        self.setpoint = setpoint
        self.dt = dt
        self.previous_error = 0
        self.integral = 0

    def update(self, current_value):
        error = self.setpoint - current_value
        self.integral += error * self.dt
        derivative = (error - self.previous_error) / self.dt
        output = self.Kp * error + self.Ki * self.integral + self.Kd * derivative
        self.previous_error = error
        print(output)
        return output

# ----------------
# ---- 冷启动 ----
def get_flow_map(dag=None, resource_info=None, offload_ptr=None):
    cold_flow_mapping = dict()
    flow = dag["flow"]

    for idx in range(len(flow)):
        taskname = flow[idx]
        if idx <= offload_ptr:
            cold_flow_mapping[taskname] = {
                "model_id": 0,
                "node_role": "host",
                "node_ip": list(resource_info["host"].keys())[0]
            }
        else:
            cold_flow_mapping[taskname] = {
                "model_id": 0,
                "node_role": "cloud",
                "node_ip": list(resource_info["cloud"].keys())[0]
            }
    
    return cold_flow_mapping

# 给定flow_map，根据kb获取处理时延
def get_process_delay(resolution=None, flow_map=None):
    sum_delay = 0.0
    for taskname in flow_map:
        pf_filename = 'profile/{}.pf'.format(taskname)
        pf_table = None
        if os.path.exists(pf_filename):
            pf_table = pd.read_table(pf_filename, sep='\t', header=None,
                                    names=['resolution', 'node_role', 'delay'])
        else:
            pf_table = pd.read_table('profile/face_detection.pf', sep='\t', header=None,
                                    names=['resolution', 'node_role', 'delay'])
        # root_logger.info(pf_table)
        node_role = 'cloud' if flow_map[taskname]['node_role'] == 'cloud' else 'edge'
        pf_table['node_role'] = pf_table['node_role'].astype(str)
        matched_row = pf_table.loc[
            (pf_table['node_role'] == node_role) & \
            (pf_table['resolution'] == resolution)
        ]
        delay = matched_row['delay'].values[0]
        root_logger.info('get profiler delay={} for taskname={} node_role={}'.format(
            delay, taskname, flow_map[taskname]['node_role']
        ))

        sum_delay += delay
    
    root_logger.info('get sum_delay={} by knowledge base'.format(sum_delay))

    return sum_delay

# TODO：给定flow_map，获取传输时延
def get_transfer_delay(resolution=None, flow_map=None, resource_info=None):
    return 0.0

# 获取总预估的时延
def get_pred_delay(conf_fps=None, cam_fps=None, resolution=None, flow_map=None, resource_info=None):
    # 给定flow_map，
    # resolution vs process_delay：基于kb
    # resolution vs transfer_delay：基于带宽计算
    # fps vs delay：比例关系

    process_sum_delay = get_process_delay(resolution=resolution, flow_map=flow_map)
    transfer_sum_delay = get_transfer_delay(resolution=resolution, flow_map=flow_map, resource_info=resource_info)

    total_delay = (process_sum_delay + transfer_sum_delay) * conf_fps / cam_fps

    return total_delay

# TODO：给定fps和resolution，结合运行时情境，获取预测时延
def get_pred_acc(conf_fps=None, cam_fps=None, resolution=None):
    return 0.95

def get_cold_start_plan(
    job_uid=None,
    dag=None,
    resource_info=None,
    user_constraint=None,
):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping
    global available_fps, available_resolution

    # 时延优先策略：算量最小，算力最大
    cold_video_conf = {
        "resolution": "360p",
        "fps": 30,
        # "ntracking": 5,
        "encoder": "JPEG",
    }
    cold_flow_mapping = dict()
    for taskname in dag["flow"]:
        cold_flow_mapping[taskname] = {
            "model_id": 0,
            "node_role": "host",
            "node_ip": list(resource_info["host"].keys())[0]
        }

    delay_ub = user_constraint["delay"]
    delay_lb = delay_ub
    acc_ub = user_constraint["accuracy"]
    acc_lb = acc_ub

    min_delay_delta = None
    min_acc_delta = None

    # 调度维度：nproc，切分点，fps，resolution
    for fps in available_fps:
        for resol in available_resolution:
            for offload_ptr in range(0, len(dag["flow"])):
                # 枚举所有策略，根据knowledge base预测时延和精度，找出符合用户约束的。
                # 若无法同时满足，优先满足时延要求。尽量满足精度要求（不要求是最优解，所以可以提前退出）
                flow_map = get_flow_map(dag=dag,
                                        resource_info=resource_info, 
                                        offload_ptr=offload_ptr)
                cam_fps = 30.0
                delay = get_pred_delay(conf_fps=fps, cam_fps=cam_fps,
                                       resolution=resol,
                                       flow_map=flow_map,
                                       resource_info=resource_info)
                acc = get_pred_acc(conf_fps=fps, cam_fps=cam_fps,
                                   resolution=resol)
                
                if delay < delay_ub:
                    # 若时延符合要求，找最符合精度要求的
                    # 防止符合要求的配置被替换
                    min_delay_delta = 0.0
                    if not min_acc_delta or min_acc_delta > abs(acc_lb - acc):
                        cold_video_conf["resolution"] = resol
                        cold_video_conf["fps"] = fps
                        cold_flow_mapping = flow_map
                        min_acc_delta = abs(acc_lb - acc)
                else:
                    # 若时延不符合要求，找出尽量符合的
                    if not min_delay_delta or min_delay_delta > abs(delay_ub - delay):
                        cold_video_conf["resolution"] = resol
                        cold_video_conf["fps"] = fps
                        cold_flow_mapping = flow_map
                        min_delay_delta = abs(delay_ub - delay)

    prev_video_conf[job_uid] = cold_video_conf
    prev_flow_mapping[job_uid] = cold_flow_mapping

    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]





# ----------------
# ---- 负反馈 ----
def adjust_parameters(output=0, job_uid=None,
                      dag=None,
                      user_constraint=None,
                      resource_info=None,
                      runtime_info=None):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping, prev_runtime_info
    global available_fps, available_resolution

    next_video_conf = prev_video_conf[job_uid]
    next_flow_mapping = prev_flow_mapping[job_uid]

    # 仅支持pipeline
    flow = dag["flow"]
    assert isinstance(flow, list), "flow not list"

    resolution_index = available_resolution.index(
        next_video_conf["resolution"])
    fps_index = available_fps.index(next_video_conf["fps"])

    level = round(output)
    if level < -3:
        level = -3
    elif level > 3:
        level = 3

    tune_msg = None

    # TODO：参照对应的边端sniffer解析运行时情境
    print('---- runtime_info in the past time slot ----')
    print('runtime_info = {}'.format(runtime_info))
    # obj_n = runtime_info['obj_n']

    if level > 0:
        # level > 0，时延满足要求
        # TODO：结合运行时情境（应用），可以调整策略以接近优化目标：
        #              若优化目标为最大化精度，在达不到要求时，可以提高fps和resolution；
        #              若优化目标为最小化云端开销，可以拉回到边端计算；
        tune_level = level
        pred_acc = get_pred_acc(conf_fps=next_video_conf['fps'], cam_fps=30.0, resolution=next_video_conf["resolution"])
        
        # 若此时预测精度达不到要求，可以提高fps和resolution
        if pred_acc < user_constraint["accuracy"]:
            # 根据不同程度的 delay-acc trade-off，在不同的delay级别调整不同的参数
            while not tune_msg and tune_level > 0:
                if tune_level == 3:
                    if fps_index + 1 < len(available_fps):
                        print(" -------- fps higher -------- (level={}, tune_msg={})".format(level, tune_msg))
                        next_video_conf["fps"] = available_fps[fps_index + 1]
                        tune_msg = "fps {} -> {}".format(available_fps[fps_index],
                                                        available_fps[fps_index + 1])

                elif tune_level == 2:
                    if resolution_index + 1 < len(available_resolution):
                        print(" -------- resolution higher -------- (level={}, tune_msg={})".format(level, tune_msg))
                        next_video_conf["resolution"] = available_resolution[resolution_index + 1]
                        tune_msg = "resolution {} -> {}".format(available_resolution[resolution_index],
                                                                available_resolution[resolution_index + 1])
                
                # 按优先级依次选择可调的配置
                if not tune_msg:
                    tune_level -= 1

    elif level < 0:
        # level < 0，时延不满足要求
        # TODO：结合运行时情境（资源），应该调整策略，以降低时延：
        #              优先分配更多资源；
        #              任务卸载到空闲节点（云/边）；
        #              最后考虑降低fps和resolution
        tune_level = level
        while not tune_msg and tune_level:
            if tune_level == -3:
                # cloud
                for taskname, task_mapping in reversed(list(next_flow_mapping.items())):
                    if task_mapping["node_role"] == "host":
                        print(" -------- send to cloud -------- (level={}, tune_msg={})".format(level, tune_msg))
                        next_flow_mapping[taskname]["node_role"] = "cloud"
                        next_flow_mapping[taskname]["node_ip"] = list(
                            resource_info["cloud"].keys())[0]
                        tune_msg = "task-{} send to cloud".format(taskname)
                        break

            if tune_level == -2:
                if fps_index > 0:
                    print(" -------- fps lower -------- (level={}, tune_msg={})".format(level, tune_msg))
                    next_video_conf["fps"] = available_fps[fps_index - 1]
                    tune_msg = "fps {} -> {}".format(available_fps[fps_index],
                                                    available_fps[fps_index - 1])

            if tune_level == -1:
                if resolution_index > 0:
                    print(" -------- resolution lower -------- (level={}, tune_msg={})".format(level, tune_msg))
                    next_video_conf["resolution"] = available_resolution[resolution_index - 1]
                    tune_msg = "resolution {} -> {}".format(available_resolution[resolution_index],
                                                            available_resolution[resolution_index - 1])
            
            # 按优先级依次选择可调的配置
            if not tune_msg:
                tune_level += 1


    prev_video_conf[job_uid] = next_video_conf
    prev_flow_mapping[job_uid] = next_flow_mapping
    prev_runtime_info[job_uid] = runtime_info

    print(prev_flow_mapping[job_uid])
    print(prev_video_conf[job_uid])
    print(prev_runtime_info[job_uid])
    root_logger.info("tune_msg: {}".format(tune_msg))
    
    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]





# -----------------
# ---- 调度入口 ----
def scheduler(
    job_uid=None,
    dag=None,
    resource_info=None,
    runtime_info=None,
    user_constraint=None,
):

    assert job_uid, "should provide job_uid for scheduler to get prev_plan of job"

    root_logger.info(
        "scheduling for job_uid-{}, runtime_info=\n{}".format(job_uid, runtime_info))

    global lastTime

    if not runtime_info or not user_constraint:
        root_logger.info("to get COLD start executation plan")
        return get_cold_start_plan(
            job_uid=job_uid,
            dag=dag,
            resource_info=resource_info,
            user_constraint=user_constraint
        )

    # ---- 若有负反馈结果，则进行负反馈调节 ----
    global prev_video_conf, prev_flow_mapping

    assert job_uid in prev_video_conf, \
        "job_uid not in prev_video_conf(keys={})".format(
            prev_video_conf.keys())
    assert job_uid in prev_flow_mapping, \
        "job_uid not in prev_video_conf(keys={})".format(
            prev_flow_mapping.keys())

    video_conf = None
    flow_mapping = None

    delay_ub = user_constraint["delay"]
    delay_lb = delay_ub

    # set pidController param
    Kp, Ki, Kd = 1, 0.1, 0.01
    setpoint = delay_ub
    dt = time.time() - lastTime
    pidControl = PIDController(Kp, Ki, Kd, setpoint, dt)

    # TODO：参照对应的边端sniffer解析运行时情境
    print('---- runtime_info in the past time slot ----')
    print('runtime_info = {}'.format(runtime_info))

    avg_delay = runtime_info['delay']
    output = pidControl.update(avg_delay)

    # adjust parameters

    return adjust_parameters(output, job_uid=job_uid,
                             dag=dag,
                             user_constraint=user_constraint,
                             resource_info=resource_info,
                             runtime_info=runtime_info)
