# 实验文件说明

## `headup_detect_delay_test_0.3_0.9_1685593125955.csv`

提交任务如下，提交后每1s轮询一次`/query/get_result`获取结果，剔除重复元素

```json
    query_body = {
        "node_addr": "192.168.56.102:5001",
        "video_id": 1,
        "pipeline": ["face_detection", "face_alignment"],
        "user_constraint": {
            "delay": 0.3,
            "accuracy": 0.9
        }
    }
```

系统配置：一台4核8G虚拟机，只用CPU推理

系统负载：单任务，虚拟机和宿主机闲置

## `20230601_15_13_41_headup_detect_delay_test_0.3_0.9.csv`

提交任务如下，提交后每1s轮询一次`/query/get_result`获取结果，剔除重复元素

```json
    query_body = {
        "node_addr": "172.27.152.177:5001",
        "video_id": 1,
        "pipeline": ["face_detection", "face_alignment"],
        "user_constraint": {
            "delay": 0.3,
            "accuracy": 0.9
        }
    }
```

系统配置：机架只用CPU推理、Jetson TX2只用GPU推理，机器内网互联

系统负载：单任务，机架和TX2均闲置
