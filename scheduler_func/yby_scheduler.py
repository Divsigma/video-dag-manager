import random

last_flow_mapping = {
    "face_detection": {
        "model_id": 0,
        "node_ip": "127.0.0.1",
    },
    "face_alignment": {
        "model_id": 0,
        "node_ip": "127.0.0.1",
    }
}

last_video_config = {
    "face_detection": {
        "resolution": "480p",
        "fps": 30,
        "encoder": "H264",
    },
    "face_alignment": {
        "resolution": "480p",
        "fps": 30,
        "encoder": "H264",
    }
}


# def load_balancer(edge_list, flow, resource_info):
#     cpu_threshold = 0.7
#     mem_threshold = 0.7

#     chosen_edge = 0

#     for edge in edge_list:
#         value = resource_info[edge]
#         info = value[flow[0]]
#         n_process, cpu_ratio, mem_ratio = info["n_process"], info["cpu_ratio"], info["mem_ratio"]
#         # print(n_process, cpu_ratio, mem_ratio)

#         if cpu_ratio < cpu_threshold and mem_ratio < mem_threshold:
#             chosen_edge = edge
#             break

#     return chosen_edge

def load_balancer(edge_list, flow, resource_info):
    load_list = [0] * len(edge_list)

    print("edge_list: {}".format(edge_list))    

    for i, edge_ip in enumerate(edge_list):
        load = 0
        cpu_load = resource_info[edge_ip][flow[0]]["cpu_ratio"]
        mem_load = resource_info[edge_ip][flow[0]]["mem_ratio"]
        load += cpu_load + mem_load
        load_list[i] = load

    print("{}".format(load_list))
    min_load = min(load_list)
    min_load_edges = [i for i, load in enumerate(
        load_list) if load == min_load]

    chosen_edge = random.choice(min_load_edges)

    return chosen_edge


def scheduler(flow, resource_info, last_plan_res, user_constraint):
    # get edge and cloud ip
    ip_list = list(resource_info.keys())
    # multi edge & single cloud
    edge_list, cloud_ip = ip_list[:-1], ip_list[-1]
    edge_list = [ip_list[0]]

    global last_flow_mapping, last_video_config

    video_conf = last_video_config
    flow_mapping = last_flow_mapping

    reso_list = ["360p", "480p", "720p", "1080p", "1800p"]
    if not last_plan_res:
        return video_conf, flow_mapping

    delay_dict = last_plan_res['delay']  # dict
    delay_list = []

    # load balancer (TBC)
    chosen_edge = load_balancer(edge_list, flow, resource_info)

    for i, stage in enumerate(flow):

        delay = delay_dict.get(flow[i], None)
        delay_list.append(delay)

        # init handle_position
        flow_mapping[stage]["node_ip"] = edge_list[chosen_edge]

    # get threshold
    delay_range = user_constraint['delay']
    acc_level = user_constraint['acc_level']

    sum_delay = sum(delay_list)

    # TBC
    # modify config according to the delay( and accuracy)
    for i, stage in enumerate(flow):
        delay = delay_list[i]
        # last_reso, last_fps, last_encoder = video_conf[stage][
        #     "resolution"], video_conf[stage]["fps"], video_conf[stage]["encoder"]
        # print(stage)

        if sum_delay > delay_range[1]:
            if delay >= delay_range[0] and delay < delay_range[1] / len(delay_list):
                continue
            # fps, model_size, resolution, handle_position, encoder
            elif delay > delay_range[1] / 2:
                p = random.uniform(0, 1)
                if p >= 0.8:
                    video_conf[stage]["fps"] = max(
                        video_conf[stage]["fps"] - 1, 1)
                    print("=====fps====")
                elif p >= 0.6:
                    flow_mapping[stage]["model_id"] = min(
                        flow_mapping[stage]["model_id"] + 1, 2)
                    print("====model size=====")
                elif p >= 0.3:
                    index = reso_list.index(video_conf[stage]["resolution"])
                    video_conf[stage]["resolution"] = reso_list[max(
                        0, index-1)]
                    print("====resolution=====")
                elif p >= 0.1:
                    flow_mapping[stage]["node_ip"] = cloud_ip
                else:
                    video_conf[stage]["encoder"] = "H264"
                    print("=====encoder======")

    last_video_config = video_conf
    last_flow_mapping = flow_mapping

    return video_conf, flow_mapping


if __name__ == '__main__':
    dag_flow = ["face_detection", "face_alignment"]

    resource_info = {
        "192.168.56.102": {
            "face_detection": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            },
            "face_alignment": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            }
        },
        "114.212.81.11": {
            "face_detection": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            },
            "face_alignment": {
                "n_process": 1,
                "cpu_ratio": 0.8,
                "mem_ratio": 0.4
            }
        },
    }

    last_plan_res = {
        "delay": {
            "face_detection": 20,
            "face_alignment": 0.5
        },
    }
    user_constraint = {
        "delay": [-1, 15],
        "acc_level": 5,
    }

    video_conf, flow_mapping = scheduler(dag_flow, resource_info,
                                         last_plan_res, user_constraint)

    print("video_config: {}\nflow_mapping: {}".format(video_conf, flow_mapping))
