# video-dag-manager

## job_tracker

### （1）大致结构

`job_tracker.py`运行后是单进程多线程的，其线程模型如下图所示（目前只有一个工作线程，详见main函数部分）。用户通过/user/submit_job接口针对视频流下发任务，任务被下发到特定的边缘节点后，在边缘节点为任务生成Job对象，然后确定Job执行计划（以下称为“调度”）、根据调度结果执行Job，一段时间后Job可以被重新调度：
![](./img/job_tracker%E7%BA%BF%E7%A8%8B%E6%A8%A1%E5%9E%8B.png)

三类线程通过Manager类统一管理：
- app线程：对外提供服务，如用户提交任务、查询视频流信息等，相关接口定义见`job_tracker.py`中app.route
- scheduler线程：负责执行调度器模块，其调度函数可替换
- 工作线程：一个工作线程模拟一个CPU核，负责若干个已生成执行计划的Job的执行。目前只有一个工作线程

Manager类：负责管理所有工作线程的Job的状态与执行状态，工作线程需要通过Manager获取一个已确定执行计划的Job来执行。Manager同时提供app线程以对外提供服务。

Job类：记录了DAG作业的执行状态机：调度计划、最后一次DAG执行的各个步骤结果、Job当前状态等

Job状态主要有三类
- UNSCHED：未生成调度计划
- EXEC：已生成调度计划，可供CPU调度执行
- DONE：终止，可能是执行完毕，也可能是执行过程中有报错

### （2）相关接口

```js
描述：获取接入到云端的节点信息
接口：GET :5000/node/get_all_status
返回结果
{
    "data": {
        "192.168.56.102:7000": {
            "video": {
                "0": {
                    "type": "traffic flow"
                },
                "1": {
                    "type": "people indoor"
                }
            }
        },
        "192.168.56.102:8000": {
            "video": {
                "0": {
                    "type": "traffic flow"
                },
                "1": {
                    "type": "people indoor"
                }
            }
        }
    },
    "status": 0
}

描述：从云端接收用户提交的任务
接口：POST :5000/user/submit_job
请求数据：dag_input中用到的字段，需要与计算服务接口文档中返回数据的字段对应
         使用的数据生成器的字段，需要与生成器接口文档中返回数据的字段对应
{
    "node_addr": "192.168.56.102:7000",
    "video_id": 1,
    "generator": "SingleFrameGenerator",
    "dag": {
        "flow": ["face_detection", "face_alignment"],
        "input": {
            "face_detection": {
                "image": "SingleFrameGenerator.image"
            },
            "face_alignment": {
                "image": "SingleFrameGenerator.image",
                "bbox": "face_detection.bbox",
                "prob": "face_detection.prob"
            }
        }
    }
}

描述：指定节点提交任务（该接口不对外直接调用）
接口：POST `:5000/node/submit_job`
请求数据：与`:5000/user/submit_job`接口几乎一致，但需要指明unique_job_id（由内部生成切分DAG后生成）
{
    "unique_job_id": "GLOBAL_ID_1.SUB_ID_1",
    "node_addr": "192.168.56.102:7000",
    "video_id": 1,
    "generator": "SingleFrameGenerator",
    "dag": {
        "flow": ["face_detection", "face_alignment"],
        "input": {
            "face_detection": {
                "image": "SingleFrameGenerator.image"
            },
            "face_alignment": {
                "image": "SingleFrameGenerator.image",
                "bbox": "face_detection.bbox",
                "prob": "face_detection.prob"
            }
        }
    }
}

描述：数据生成器
名称：SingleFrameGenerator
返回数据
{
    "seq":
    "image": // RGB图像经过python3的cv2.imencode编码后的字节流
}
```

## service_demo
```js
描述：提供D计算服务
接口：POST :5500/service/face_detection
请求数据：作为dag中接收数据的服务，其请求数据格式需要于指定的数据生成器的返回数据格式一致
         如选择SingleFrameGenerator作为数据生成器，则计算服务需要处理数据
{
    "image": // RGB图像经过python3的cv2.imencode编码后的字节流
}
返回数据
{
    "bbox":
    "prob":
}

描述：提供C计算服务
接口：POST :5500/service/face_alignment
请求数据：
{
    "image":
    "bbox":
    "prob":
}
返回数据：
{
    "head_pose":
}
```

## 调度器函数接口
调度器应封装为一个函数，决定视频流分析配置、并将DAG Job中的dag.flow的各个任务映射到节点。

函数参数：

（1）待映射/调度的DAG Job
- 参考`POST :5000/node/submit_job`端口的`dag`下的`flow`字段
```js
flow = ["face_detection", "face_alignment"]
```

（2）DAG的输入数据信息（暂不考虑）
- 参考“数据生成器”的返回数据（如，SingleFrameGenerator）
- 也可以是基于对本次调度的数据流的数据评估信息（如图片复杂度、图片数据大小）
```js
generator_output = {
    "seq":
    "image":
}
```


（3）资源和服务情况
- 各机器CPU、内存、GPU情况
- 各机器服务的请求地址
- 当前节点与其余节点的上行带宽/下行带宽
```js
// TBD
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
```

（4）上一轮调度方案的执行结果（若上一轮调度包含多帧，则取各帧数值结果的平均）
- 一帧图像经过DAG推理的总时延
```js
last_plan_res = {
    "delay": {
        "face_detection": 20,
        "face_alignment": 0.5
    },
    "iou": {
        "face_detection"
    },
    "mae": {
        "face_alignment"
    }
}

last_plan_res = {
    "face_detection": {
        "iou": 20,
        "delay"
    },
    "face_alignment": {
        "mae": 20,
        "delay"
    },
}

```

（5）用户约束
- 时延范围
- 精度反馈
```js
user_constraint = {
    "delay": [-1, 50],
    "acc_level": 5,  // 用户给出的精度评级：0~5精确等级递增
}
```


函数返回：

（1）视频配置
- 分辨率
- 跳帧率/fps
- 编码方式
```js
video_conf = {
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
```

（2）DAG执行策略
- 字典中的key需要与传入的DAG Job中`flow`各字段匹配
```js
flow_mapping = {
    "face_detection": {
        "model_id": 0,  // 大模型、中模型、小模型
        "node_ip": "192.168.56.102",  // 映射的节点
    },
    "face_alignment": {
        "model_id": 0,
        "node_ip": "114.212.81.11",
    }
}
```