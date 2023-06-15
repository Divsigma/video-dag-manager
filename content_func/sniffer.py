import numpy as np

class Sniffer():
    CONTENT_ELE_MAXN = 50

    def __init__(self, job_uid):
        self.job_uid = job_uid
        self.runtime_pkg_list = dict()

    # TODO：根据taskname解析output_ctx，得到运行时情境
    def sniff(self, taskname, output_ctx):
        if taskname == 'end_pipe':
            if 'delay' not in self.runtime_pkg_list:
                self.runtime_pkg_list['delay'] = list()
            
            if len(self.runtime_pkg_list['delay']) > Sniffer.CONTENT_ELE_MAXN:
                del self.runtime_pkg_list['delay'][0]
            self.runtime_pkg_list['delay'].append(output_ctx['delay'])

        # 对face_detection的结果，提取运行时情境
        # TODO：目标数量、目标大小、目标速度
        if taskname == 'face_detection' :
            # 定义运行时情境字段
            if 'obj_n' not in self.runtime_pkg_list:
                self.runtime_pkg_list['obj_n'] = list()
            if 'obj_size' not in self.runtime_pkg_list:
                self.runtime_pkg_list['obj_size'] = list()

            # 更新各字段序列（防止爆内存）
            if len(self.runtime_pkg_list['obj_n']) > Sniffer.CONTENT_ELE_MAXN:
                del self.runtime_pkg_list['obj_n'][0]
            self.runtime_pkg_list['obj_n'].append(len(output_ctx['faces']))

            obj_size = 0
            for x_min, y_min, x_max, y_max in output_ctx['bbox']:
                # TODO：需要依据分辨率转化
                obj_size += (x_max - x_min) * (y_max - y_min)
            obj_size /= len(output_ctx['bbox'])

            if len(self.runtime_pkg_list['obj_size']) > Sniffer.CONTENT_ELE_MAXN:
                del self.runtime_pkg_list['obj_size'][0]
            self.runtime_pkg_list['obj_size'].append(obj_size)
        
        # 对car_detection的结果，提取目标数量
        # TODO：目标数量、目标大小、目标速度
        if taskname == 'car_detection':
            # 定义运行时情境字段
            if 'obj_n' not in self.runtime_pkg_list:
                self.runtime_pkg_list['obj_n'] = list()

            # 更新各字段序列（防止爆内存）
            if len(self.runtime_pkg_list['obj_n']) > Sniffer.CONTENT_ELE_MAXN:
                del self.runtime_pkg_list['obj_n'][0]
            self.runtime_pkg_list['obj_n'].append(
                sum(list(output_ctx['count_result'].values()))
            )
            

            
    def describe_runtime(self):
        # TODO：聚合情境感知参数的时间序列，给出预估值/统计值
        runtime_desc = dict()
        for k, v in self.runtime_pkg_list.items():
            runtime_desc[k] = sum(v) * 1.0 / len(v)
        
        # 获取场景稳定性
        if 'obj_n' in self.runtime_pkg_list.keys():
            runtime_desc['obj_stable'] = True if np.std(self.runtime_pkg_list['obj_n']) < 0.3 else False

        # 每次调用agg后清空
        self.runtime_pkg_list = dict()
        
        return runtime_desc