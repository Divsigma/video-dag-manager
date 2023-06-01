import requests
import time
import csv
import os
import datetime

if __name__ == "__main__":
    sess = requests.Session()

    # 提交请求
    query_body = {
        "node_addr": "127.0.0.1:5001",
        "video_id": 1,
        "pipeline": ["face_detection", "face_alignment"],
        "user_constraint": {
            "delay": 0.3,
            "accuracy": 0.9
        }
    }
    r = sess.post(url="http://192.168.56.102:5000/query/submit_query",
                  json=query_body)
    
    resp = r.json()
    query_id = resp["query_id"]
    

    filename = datetime.datetime.now().strftime('%Y%m%d_%H_%M_%S') + \
        '_' + os.path.basename(__file__).split('.')[0] + \
        '_' + str(query_body['user_constraint']['delay']) + \
        '_' + str(query_body['user_constraint']['accuracy']) + \
        '.csv'
    
    with open(filename, 'w', newline='') as fp:
        fieldnames = ['n_loop', 'frame_id', 'total', 'up', 'fps', 'resolution', 'delay', 'face_detection', 'face_alignment']
        wtr = csv.DictWriter(fp, fieldnames=fieldnames)
        wtr.writeheader()

        written_n_loop = dict()

        # 轮询结果+落盘
        while True:
            r = None
            try:
                time.sleep(1)
                print("post one query request")
                r = sess.get(url="http://192.168.56.102:5000/query/get_result/{}".format(query_id))
                if not r.json():
                    continue
                resp = r.json()

                res_list = resp['appended_result']
                plan = resp['latest_result']['plan']
                plan_result = resp['latest_result']['plan_result']

                fps = plan['video_conf']['fps']
                resolution = plan['video_conf']['resolution']
                delay = sum(plan_result['delay'].values())

                fd_role = plan['flow_mapping']['face_detection']['node_role']
                fa_role = plan['flow_mapping']['face_alignment']['node_role']

                for res in res_list:
                    n_loop, frame_id, total, up = res['n_loop'], res['frame_id'], res['count_result']['total'], res['count_result']['up']
                    row = {
                        'n_loop': n_loop,
                        'frame_id': frame_id,
                        'total': total,
                        'up': up,
                        'fps': fps,
                        'resolution': resolution,
                        'delay': delay,
                        'face_detection': fd_role,
                        'face_alignment': fa_role
                    }
                    if n_loop not in written_n_loop:
                        wtr.writerow(row)
                        written_n_loop[n_loop] = 1
                    
                print("written one query response, len written_n_loop={}".format(len(written_n_loop.keys())))

            except Exception as e:
                if r:
                    print("got serv result: {}".format(r.text))
                print("caught exception: {}".format(e), exc_info=True)
                break