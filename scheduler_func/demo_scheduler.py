from logging_utils import root_logger

prev_video_conf = {
    "resolution": "480p",
    "fps": 30,
    "nskip": 5,
    "encoder": "H264",
}

prev_flow_mapping = {
    "face_detection": {
        "model_id": 0,
        "node_role": "host",
        "node_ip": "192.168.56.102"
    },
    "face_alignment": {
        "model_id": 0,
        "node_role": "host",
        "node_ip": "192.168.56.102"
    }
}

def get_next_exec_plan(
    dag=None,
    resource_info=None,
    user_constraint=None,
    faster=True
):
    global prev_video_conf, prev_flow_mapping

    next_video_conf = prev_video_conf
    next_flow_mapping = prev_flow_mapping

    # 枚举策略，选出提速最快的方案。【变量离散化，每次只改变一个维度】。策略变量：
    # 视频流配置变量=[处理时长，帧率，跳帧率，像素点个数]，记作[t, fps, skpps, npxpf]
    # 各模型配置变量=[模型计算量]，记作[nparam]（代表计算量）
    # 各节点、各服务资源配置=[带宽，cpu单周期占有率，cpu单周期计算次数]，记作[bw, cpu_ratio, cpu_ntimes]
    # 任务t是否卸载到节点j执行，记作O(t,j)

    # 各配置变量与时延关系（用最简单的knowledge base预估时延）：
    # 正相关（+t）：fps、npxpf、各服务nparam
    # 负相关（-t）：skpps、各服务cpu_ratio、各服务cpu_ntimes
    # 不定：边+云协同的切分点，【约束】————尽量先在边做（为了安全性），
    #      所以一开始先尽量压榨边的算力，压榨完了再上云。资源&视频流配置更改后，重新按此逻辑压榨

    # 仅支持pipeline
    flow = dag["flow"]
    assert isinstance(flow, list)
    flow_input = dag["input"]
    input_deli = dag["input_deliminator"]

    available_fps = [24, 30, 60, 120]
    # available_npxpf = [480*360, 858*480, 1280*720, 1920*1080]
    available_resolution = ["360p", "480p", "720p", "1080p"]

    # 记用户时延要求[lb, ub]
    # 1、若时延>ub：根据预估配置-时延正负相关关系，先提高资源配置、降低视频流配置，尽量减少当前协同方式下处理时延
    #                压榨完配置还不行，开始调整协同方式，逐步卸载到云端处理
    # 2、若时延<lb：同样根据关系，先降低资源配置、提高视频流配置，在保证服务质量情况下降低资源消耗
    #            调整完配置若发现超时，进入情况1，情况一会重新提高资源配置（或降低视频流配置）

    resolution_index = available_resolution.index(next_video_conf["resolution"])
    fps_index = available_fps.index(next_video_conf["fps"])

    if faster:
        if resolution_index > 0:
            next_video_conf["resolution"] = available_resolution[resolution_index - 1]
        elif fps_index > 0:
            next_video_conf["fps"] = available_fps[fps_index - 1]
        else:
            # 在dag中从后往前，逐步卸载到云（不考虑边边协同）
            for taskname, task_mapping in reversed(next_flow_mapping.items()):
                if task_mapping["node_role"] == "host":
                    next_flow_mapping[taskname]["node_role"] = "cloud"
                    next_flow_mapping[taskname]["node_ip"] = list(resource_info["cloud"].keys())[0]
                    break
    else:
        if resolution_index + 1 < len(available_resolution):
            next_video_conf["resolution"] = available_resolution[resolution_index + 1]
        elif fps_index + 1 < len(available_fps):
            next_video_conf["fps"] = available_fps[fps_index + 1]
        else:
            # 在dag中从前往后，逐步回拉到边（不考虑边边协同）
            for taskname, task_mapping in next_flow_mapping.items():
                if task_mapping["node_role"] != "host":
                    next_flow_mapping[taskname]["node_role"] = "host"
                    next_flow_mapping[taskname]["node_ip"] = list(resource_info["host"].keys())[0]
                    break

    prev_video_conf = next_video_conf
    prev_flow_mapping = next_flow_mapping
    return prev_video_conf, prev_flow_mapping

def get_cold_start_plan(
    dag=None,
    resource_info=None,
    user_constraint=None,
):
    
    global prev_video_conf, prev_flow_mapping

    return prev_video_conf, prev_flow_mapping

def scheduler(
    dag=None,
    resource_info=None, 
    last_plan_res=None,
    user_constraint=None,
):
    
    root_logger.info("last_plan_res=\n{}".format(last_plan_res))

    # ---- 若无负反馈结果，则进行冷启动 ----
    if not last_plan_res:
        # 基于knowledge base给出一个方案？
        # 选择最高配置？
        # 期望：根据资源情况决定一个合理的配置，以便负反馈快速收敛到稳定方案
        return get_cold_start_plan(
            dag=dag,
            resource_info=resource_info,
            user_constraint=user_constraint
        )
    
    # ---- 若有负反馈结果，则进行负反馈调节 ----
    video_conf = None
    flow_mapping = None

    # 三方面决策：协同策略、视频流配置、资源配置
    # 根据“供需”关系调整

    # 时延=各阶段（推理时延+传输时延）
    
    # 负反馈迭代，尽量将时延调整到用户指定的时延范围
    delay_lb = user_constraint["delay"][0]
    delay_ub = user_constraint["delay"][1]
    if sum(last_plan_res["delay"].values()) > delay_ub:
        # 上次调度时延 > 用户约束上界，提高处理速度
        root_logger.info("to get FASTER executation plan")
        return get_next_exec_plan(
            dag=dag,
            resource_info=resource_info,
            user_constraint=user_constraint,
            faster=True
        )
    elif sum(last_plan_res["delay"].values()) < delay_lb:
        # 降低处理速度
        root_logger.info("to get slower executation plan")
        return get_next_exec_plan(
            dag=dag,
            resource_info=resource_info,
            user_constraint=user_constraint,
            faster=False
        )
    
    global prev_video_conf, prev_flow_mapping

    return prev_video_conf, prev_flow_mapping