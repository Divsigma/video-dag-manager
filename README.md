# video-dag-manager

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
请求数据
{
    "node_addr": "192.168.56.102:7000",
    "video_id": 1,
    "dag": ["D", "C"]
}
```
