from logging_utils import root_logger
import pandas as pd
import os

prev_video_conf = dict()

prev_flow_mapping = dict()

def get_cold_start_plan(
    job_uid=None,
    dag=None,
    resource_info=None,
    user_constraint=None,
):
    assert job_uid, "should provide job_uid"

    global prev_video_conf, prev_flow_mapping

    # 黄金配置
    cold_video_conf = {
        "resolution": "1080p",
        "fps": 120,
        "encoder": "JPEG",
    }
    cold_flow_mapping = dict()
    for taskname in dag["flow"]:
        cold_flow_mapping[taskname] = {
            "model_id": 0,
            "node_role": "cloud",
            "node_ip": list(resource_info["cloud"].keys())[0]
        }

    prev_video_conf[job_uid] = cold_video_conf
    prev_flow_mapping[job_uid] = cold_flow_mapping

    return prev_video_conf[job_uid], prev_flow_mapping[job_uid]

def scheduler(
    job_uid=None,
    dag=None,
    resource_info=None, 
    last_plan_res=None,
    user_constraint=None,
):
    
    assert job_uid, "should provide job_uid for scheduler to get prev_plan of job"

    root_logger.info("scheduling for job_uid-{}, last_plan_res=\n{}".format(job_uid, last_plan_res))


    # 返回黄金配置
    return get_cold_start_plan(
        job_uid=job_uid,
        dag=dag,
        resource_info=resource_info,
        user_constraint=user_constraint
    )