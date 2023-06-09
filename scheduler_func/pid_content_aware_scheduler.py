from logging_utils import root_logger

import time

prev_video_conf = dict()

prev_flow_mapping = dict()

prev_runtime_info = dict()


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

    # def adjust_parameters(self, output):
        level = round(output)
        if level < -3:
            level = -3
        elif level > 3:
            level = 3

        # Adjust processing position, model size, and video configuration based on the level
        if level == -3:
            print("send to edge")
        elif level == -2:
            print("model_size higher")
        elif level == -1:
            print("video conf higher")
        elif level == 0:
            pass
        elif level == 1:
            # video_conf↓
            print("video conf lower")
        elif level == 2:
            # model_size ↓
            print("model_size lower")
            pass
        elif level == 3:
            # cloud
            print("send to cloud")


def get_cold_start_plan(
    job_uid=None,
    dag=None,
    resource_info=None,
    user_constraint=None,
):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping
 
    from scheduler_func import demo_scheduler

    cold_video_conf, cold_flow_mapping = demo_scheduler.get_cold_start_plan(job_uid=job_uid,
                                                                            dag=dag,
                                                                            resource_info=resource_info,
                                                                            user_constraint=user_constraint)

    prev_video_conf[job_uid] = cold_video_conf
    prev_flow_mapping[job_uid] = cold_flow_mapping

    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]


def adjust_parameters(output=0, job_uid=None,
                      dag=None,
                      resource_info=None,
                      runtime_info=None):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping, prev_runtime_info

    next_video_conf = prev_video_conf[job_uid]
    next_flow_mapping = prev_flow_mapping[job_uid]

    # 仅支持pipeline
    flow = dag["flow"]
    assert isinstance(flow, list), "flow not list"

    available_fps = [1, 5, 10, 20, 30]
    # available_fps = [10, 20, 30]
    # available_npxpf = [480*360, 858*480, 1280*720, 1920*1080]
    available_resolution = ["360p", "480p", "720p", "1080p"]

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
        # level > 0，可以调整策略：提高fps和resolution，在边端计算以降低云端压力
        tune_level = level
        while not tune_msg and tune_level > 0:
            if tune_level == 3:
                # 根据运行时情境，防止震荡
                # if job_uid in prev_runtime_info and \
                #    abs(runtime_info['obj_n'] - prev_runtime_info[job_uid]['obj_n']) > 3:
                for taskname, task_mapping in next_flow_mapping.items():
                    if task_mapping["node_role"] != "host":
                        print(" -------- back to edge -------- (level={}, tune_msg={})".format(level, tune_msg))
                        next_flow_mapping[taskname]["node_role"] = "host"
                        next_flow_mapping[taskname]["node_ip"] = list(
                            resource_info["host"].keys())[0]
                        tune_msg = "task-{} back to edge".format(taskname)
                        break

            elif tune_level == 2:
                if fps_index + 1 < len(available_fps):
                    print(" -------- fps higher -------- (level={}, tune_msg={})".format(level, tune_msg))
                    next_video_conf["fps"] = available_fps[fps_index + 1]
                    tune_msg = "fps {} -> {}".format(available_fps[fps_index],
                                                    available_fps[fps_index + 1])

            elif tune_level == 1:
                if resolution_index + 1 < len(available_resolution):
                    print(" -------- resolution higher -------- (level={}, tune_msg={})".format(level, tune_msg))
                    next_video_conf["resolution"] = available_resolution[resolution_index + 1]
                    tune_msg = "resolution {} -> {}".format(available_resolution[resolution_index],
                                                            available_resolution[resolution_index + 1])
            
            # 按优先级依次选择可调的配置
            if not tune_msg:
                tune_level -= 1

    elif level < 0:
        # level < 0，调整策略以降低时延：降低fps和resolution，卸载到云端
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
                tune_level -= 1


    prev_video_conf[job_uid] = next_video_conf
    prev_flow_mapping[job_uid] = next_flow_mapping
    prev_runtime_info[job_uid] = runtime_info

    print(prev_flow_mapping[job_uid])
    print(prev_video_conf[job_uid])
    print(prev_runtime_info[job_uid])
    root_logger.info("tune_msg: {}".format(tune_msg))
    
    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]


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

    # ---- 若无负反馈结果或用户无约束，则进行冷启动 ----
    # 当前方式：“时延优先”模式的冷启动（算量最低，算力未决策）
    if not runtime_info or not user_constraint:
        # 基于knowledge base给出一个方案？
        # 选择最高配置？
        # 期望：根据资源情况决定一个合理的配置，以便负反馈快速收敛到稳定方案
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
    # delay_ub = float(user_constraint["delay"])
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
                             resource_info=resource_info,
                             runtime_info=runtime_info)
