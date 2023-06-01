from logging_utils import root_logger

import time

prev_video_conf = dict()

prev_flow_mapping = dict()


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

    # 时延优先策略：算量最小，算力最大
    cold_video_conf = {
        "resolution": "360p",
        "fps": 24,
        # "ntracking": 5,
        # "encoder": "JPEG",
    }

    cold_flow_mapping = dict()

    for taskname in dag["flow"]:
        cold_flow_mapping[taskname] = {
            "model_id": 0,
            "node_role": "host",
            "node_ip": list(resource_info["host"].keys())[0]
        }
    
    from scheduler_func import demo_scheduler
    # from demo_scheduler import get_cold_start_plan as demo_get_cold_start_plan

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
                      user_constraint=None,):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping

    next_video_conf = prev_video_conf[job_uid]
    next_flow_mapping = prev_flow_mapping[job_uid]

    # 仅支持pipeline
    flow = dag["flow"]
    assert isinstance(flow, list), "flow not list"

    available_fps = [24, 30, 60, 120]
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

    # Adjust processing position, fps, and resolution based on the level
    if level == 3:
        print("====================================send to edge====================================")
        for taskname, task_mapping in next_flow_mapping.items():
            if task_mapping["node_role"] != "host":
                next_flow_mapping[taskname]["node_role"] = "host"
                next_flow_mapping[taskname]["node_ip"] = list(
                    resource_info["host"].keys())[0]
                break

    elif level == 2:
        print("====================================fps higher====================================")
        if fps_index + 1 < len(available_fps):
            next_video_conf["fps"] = available_fps[fps_index + 1]

    elif level == 1:
        print("====================================resolution higher====================================")
        if resolution_index + 1 < len(available_resolution):
            next_video_conf["resolution"] = available_resolution[resolution_index + 1]

    elif level == 0:
        pass

    elif level == -1:
        print("===================================resolution lower====================================")
        if resolution_index > 0:
            next_video_conf["resolution"] = available_resolution[resolution_index - 1]

    elif level == -2:
        print("====================================fps lower====================================")
        if fps_index > 0:
            next_video_conf["fps"] = available_fps[fps_index - 1]

    elif level == -3:
        # cloud
        print("====================================send to cloud====================================")
        for taskname, task_mapping in reversed(next_flow_mapping.items()):
            if task_mapping["node_role"] == "host":
                next_flow_mapping[taskname]["node_role"] = "cloud"
                next_flow_mapping[taskname]["node_ip"] = list(
                    resource_info["cloud"].keys())[0]
                break

    prev_video_conf[job_uid] = next_video_conf
    prev_flow_mapping[job_uid] = next_flow_mapping
    print(prev_flow_mapping)
    print(prev_video_conf)
    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]


def scheduler(
    job_uid=None,
    dag=None,
    resource_info=None,
    last_plan_res=None,
    user_constraint=None,
):

    assert job_uid, "should provide job_uid for scheduler to get prev_plan of job"

    root_logger.info(
        "scheduling for job_uid-{}, last_plan_res=\n{}".format(job_uid, last_plan_res))

    global lastTime

    # ---- 若无负反馈结果或用户无约束，则进行冷启动 ----
    # 当前方式：“时延优先”模式的冷启动（算量最低，算力未决策）
    if not last_plan_res or not user_constraint:
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

    # 三方面决策：协同策略、视频流配置、资源配置
    # 根据“供需”关系调整

    # 时延=各阶段（推理时延+传输时延）

    # 负反馈迭代，尽量将时延调整到用户指定的时延范围
    # delay_lb = user_constraint["delay"][0]
    # delay_ub = user_constraint["delay"][1]

    delay_ub = user_constraint["delay"]
    # delay_ub = float(user_constraint["delay"])
    delay_lb = delay_ub

    # set pidController param
    Kp, Ki, Kd = 1, 0.1, 0.01
    setpoint = delay_ub
    dt = time.time() - lastTime
    pidControl = PIDController(Kp, Ki, Kd, setpoint, dt)

    output = pidControl.update(sum(last_plan_res["delay"].values()))

    # adjust parameters

    return adjust_parameters(output, job_uid=job_uid,
                             dag=dag,
                             resource_info=resource_info,
                             user_constraint=user_constraint)
