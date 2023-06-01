import sys
sys.path.append('..')
import cv2
import numpy as np
import time
import torch
import torch.nn as nn
from torch.autograd import Variable
from torchvision import transforms
import torch.backends.cudnn as cudnn
import torchvision
import torch.nn.functional as F
from PIL import Image
# import hopenet
# import hopenetlite_v2
# import utils2

from . import hopenet
from . import hopenetlite_v2
from . import utils2
from .utils import utils

import os

def use_gpu(dev):
    ret = False

    if dev[:4] == 'cuda':
        if torch.cuda.is_available():
            ret = True
            print('using gpu ({})'.format(dev))
        else:
            print('torch.cuda.is_available() == False')
    else:
        print('device ({}) is not cuda:*'.format(dev))

    return ret

class FaceAlignmentCNN:

    def __init__(self, args):

        # for loading model at relative path
        ori_dir = os.getcwd()
        os.chdir(os.path.dirname(__file__))

        cudnn.enabled = True

        self.__gpu = args['device']
        model_path = args['model_path']

        print('[{}] Loading model from {}...'.format(__name__, args['model_path']))

        if not args['lite_version']:
            self.__model = hopenet.Hopenet(torchvision.models.resnet.Bottleneck, [3, 4, 6, 3], 66)
        else:
            self.__model = hopenetlite_v2.HopeNetLite() # lite version

        # saved_state_dict = torch.load(model_path)
        saved_state_dict = torch.load(model_path, map_location=torch.device('cpu'))
        self.__model.load_state_dict(saved_state_dict)

        # image preprocess
        self.__transformations = transforms.Compose([transforms.Resize(224),
        transforms.CenterCrop(224), transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])

        if use_gpu(self.__gpu):
          self.__model.cuda(self.__gpu)
        else:
          self.__model.eval()

        print('[{}] Model loaded.'.format(__name__))

        # Test torchscript model
        #print('Convert to torch script model...')
        #example = torch.rand(1, 3, 224, 224)
        #example = example.cuda(self.__gpu)
        #self.__script_model = torch.jit.trace(self.__model, example)
        #self.__script_model = torch.jit.script(self.__model)

        idx_tensor = [idx for idx in range(66)]
        if use_gpu(self.__gpu):
            self.__idx_tensor = torch.FloatTensor(idx_tensor).cuda(self.__gpu)
        else:
            self.__idx_tensor = torch.FloatTensor(idx_tensor)

        self.__batch_size = args['batch_size']


        # for loading model at relative path
        os.chdir(ori_dir)

    def __call__(self, input_ctx):
        '''
        An implementation of head pose estimation by solvePnP.
        :param image:
        :param box:
        :return: euler angles
        '''

        head_pose = []

        # ---- 基于image和bbox生成face，然后计算head_pose
        # image = input_ctx['image']
        # bbox = input_ctx['bbox']
        # prob = input_ctx['prob']
        # # image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        # height, width, _ = image.shape
        # #for i in range(50):
        # print('[{}] len(bbox)={}'.format(__name__, len(bbox)))
        # for x_min, y_min, x_max, y_max in bbox:

        #     x_min = int(max(x_min, 0))
        #     y_min = int(max(y_min, 0))
        #     x_max = int(min(width, x_max))
        #     y_max = int(min(height, y_max))

        #     #print('[{}] Face scale: {} {}'.format(__name__, x_max - x_min, y_max - y_min))

        #     face = Image.fromarray(image[y_min:y_max, x_min:x_max])
        #     face = self.__transformations(face)
        #     face = face.view(1, face.shape[0], face.shape[1], face.shape[2])
  
        #     if use_gpu(self.__gpu):
        #         face = Variable(face).cuda(self.__gpu)
        #     else:
        #         face = Variable(face)

        #     yaw, pitch, roll = self.__model(face)
        #     yaw_predicted = F.softmax(yaw, dim=1)
        #     pitch_predicted = F.softmax(pitch, dim=1)
        #     roll_predicted = F.softmax(roll, dim=1)
        #     yaw_predicted = torch.sum(yaw_predicted.data[0] * self.__idx_tensor) * 3 - 99
        #     pitch_predicted = torch.sum(pitch_predicted.data[0] * self.__idx_tensor) * 3 - 99
        #     roll_predicted = torch.sum(roll_predicted.data[0] * self.__idx_tensor) * 3 - 99
        #     head_pose.append([yaw_predicted, pitch_predicted, roll_predicted, (x_min + x_max) / 2, (y_min + y_max) / 2, (y_max - y_min) / 2])


        # ---- 基于faces结果计算head_pose
        faces = input_ctx['faces']
        bbox = input_ctx['bbox']
        prob = input_ctx['prob']
        print('[{}] len(bbox)={}'.format(__name__, len(bbox)))
        for face in faces:

            #print('[{}] Face scale: {} {}'.format(__name__, x_max - x_min, y_max - y_min))

            face = Image.fromarray(face)
            face = self.__transformations(face)
            face = face.view(1, face.shape[0], face.shape[1], face.shape[2])
  
            if use_gpu(self.__gpu):
                face = Variable(face).cuda(self.__gpu)
            else:
                face = Variable(face)

            yaw, pitch, roll = self.__model(face)
            yaw_predicted = F.softmax(yaw, dim=1)
            pitch_predicted = F.softmax(pitch, dim=1)
            roll_predicted = F.softmax(roll, dim=1)
            yaw_predicted = torch.sum(yaw_predicted.data[0] * self.__idx_tensor) * 3 - 99
            pitch_predicted = torch.sum(pitch_predicted.data[0] * self.__idx_tensor) * 3 - 99
            roll_predicted = torch.sum(roll_predicted.data[0] * self.__idx_tensor) * 3 - 99
            # head_pose.append([yaw_predicted, pitch_predicted, roll_predicted, (x_min + x_max) / 2, (y_min + y_max) / 2, (y_max - y_min) / 2])
            head_pose.append([yaw_predicted, pitch_predicted, roll_predicted, (0 + face.shape[1]) / 2, (0 + face.shape[0]) / 2, (face.shape[0] - 0) / 2])

        output_ctx = {}
        # output_ctx['image'] = image
        # output_ctx['bbox'] = bbox
        # if use_gpu(self.__gpu):
        #     output_ctx['head_pose'] = torch.Tensor(head_pose).cpu().tolist()
        # else:
        #     output_ctx['head_pose'] = np.array(head_pose, dtype=np.float32).tolist()

        up, total, thres = 0, len(bbox), -10
        bbox = np.reshape(bbox, (-1, 4)).astype(int)
        if use_gpu(self.__gpu):
            head_pose = torch.Tensor(head_pose).cpu().tolist()
        else:
            head_pose = np.array(head_pose, dtype=np.float32).tolist()
        for yaw, pitch, roll, tdx, tdy, size in head_pose:
            if pitch > thres:
                up += 1
        output_ctx["count_result"] = {"up": up, "total": total}

        # ---- 渲染最终结果 ----
        # # render output["image"]
        # axis, up, total, thres = [], 0, 0, -10

        # # render bbox
        # bbox = np.reshape(bbox, (-1, 4)).astype(int)
        # for i, box in enumerate(bbox):
        #     total += 1
        #     cv2.rectangle(image, (box[0], box[1]), (box[2], box[3]),
        #                     (255, 0, 0), 4)

        # # render head pose
        # for yaw, pitch, roll, tdx, tdy, size in head_pose:
        #     if pitch > thres:
        #         up += 1
        #     ax = utils.draw_axis(image, yaw, pitch, roll, tdx=tdx, tdy=tdy, size=size)
        #     axis.append(ax)
        # for ax in axis:
        #     ax = [int(x) for x in ax]
        #     cv2.line(image, (ax[0], ax[1]), (ax[2], ax[3]), (0, 0, 255), 3)
        #     cv2.line(image, (ax[0], ax[1]), (ax[4], ax[5]), (0, 255, 0), 3)
        #     cv2.line(image, (ax[0], ax[1]), (ax[6], ax[7]), (0, 255, 255), 2)

        # # render stats
        # cv2.putText(image, 'Up: {} Total: {}'.format(up, total), (50, 50),
        #             cv2.FONT_HERSHEY_SIMPLEX, 1, (0, 255, 0), 2)
        
        # # output_ctx["image"] = image
        # output_ctx["count_result"] = {"up": up, "total": total}

        return output_ctx

    def forward(self, image, bbox, prob):
        '''
        An implementation of head pose estimation by solvePnP.
        :param image:
        :param box:
        :return: euler angles
        '''
        head_pose = []
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        height, width, _ = image.shape

        i = 0
        while i < len(bbox):
            if use_gpu(self.__gpu):
                face_batch = torch.zeros([self.__batch_size, 3, 224, 224], device='cuda')
            else:
                face_batch = torch.zeros([self.__batch_size, 3, 224, 224], device='cpu')

            j = 0
            face_location = []
            while j < self.__batch_size and i + j < len(bbox):
                x_min, y_min, x_max, y_max = bbox[i + j]
                x_min = int(max(x_min, 0))
                y_min = int(max(y_min, 0))
                x_max = int(min(x_max, width))
                y_max = int(min(y_max, height))
                face_location.append([x_min, y_min, x_max, y_max])
                face = Image.fromarray(image[y_min:y_max, x_min:x_max])
                face = self.__transformations(face)
                face = face.view(1, face.shape[0], face.shape[1], face.shape[2])

                if use_gpu(self.__gpu):
                    face = Variable(face).cuda(self.__gpu)
                else:
                    face = Variable(face)

                face_batch[j] = face
                j += 1

            yaw, pitch, roll = self.__model(face_batch)
            for k, (x_min, y_min, x_max, y_max) in enumerate(face_location):
                yaw_predicted = F.softmax(yaw[k].unsqueeze(0), dim=1)
                pitch_predicted = F.softmax(pitch[k].unsqueeze(0), dim=1)
                roll_predicted = F.softmax(roll[k].unsqueeze(0), dim=1)
                yaw_predicted = torch.sum(yaw_predicted.data[0] * self.__idx_tensor) * 3 - 99
                pitch_predicted = torch.sum(pitch_predicted.data[0] * self.__idx_tensor) * 3 - 99
                roll_predicted = torch.sum(roll_predicted.data[0] * self.__idx_tensor) * 3 - 99
                head_pose.append([yaw_predicted, pitch_predicted, roll_predicted, (x_min + x_max) / 2, (y_min + y_max) / 2, (y_max - y_min) / 2])

            i += j

        return head_pose
