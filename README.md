# video-dag-manager

## job_tracker
```js
描述：获取接入到云端的节点信息
接口：GET :6000/node/get_all_status
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

描述：获取接入到云端的节点信息
接口：POST :6000/user/submit_job
请求数据：dag_input中用到的字段，需要与计算服务接口文档中返回数据的字段对应
         使用的数据生成器的字段，需要与生成器接口文档中返回数据的字段对应
{
    "node_addr": "192.168.56.102:7000",
    "video_id": 1,
    "generator": "SingleFrameGenerator",
    "dag": {
        "flow": ["D", "C"],
        "input": {
            "D": {
                "image": "SingleFrameGenerator.image"
            },
            "C": {
                "image": "SingleFrameGenerator.image",
                "bbox": "D.bbox",
                "prob": "D.prob"
            }
        }
    }
}

描述：数据生成器
名称：SingleFrameGenerator
返回数据
{
    "image": // RGB图像经过python3的cv2.imencode编码后的字节流
}
```

## service_demo
```js
描述：提供D计算服务
接口：POST :5000/service/D
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
接口：POST :5000/service/C
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