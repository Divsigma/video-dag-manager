import argparse

def parse_args():

    parser = argparse.ArgumentParser()

    # server ip and port
    parser.add_argument('--server_ip', default='127.0.0.1', type=str,
                        help='server ip')
    parser.add_argument('--server_port', default=12345, type=int,
                       help='server port')

    # face detection model
    parser.add_argument('--net_type', default='mb_tiny_RFB_fd', type=str,
                        help='network structure : mb_tiny_RFB_fd (higher precision) or mb_tiny_fd (faster)')
    parser.add_argument('--input_size', default=480, type=int,
                        help='define network input size : 128/160/320/480/640/1280')
    parser.add_argument('--threshold', default=0.7, type=float,
                        help='score threshold')
    parser.add_argument('--candidate_size', default=1500, type=int,
                        help='nms candidate size')

    # head pose estimation model
    parser.add_argument('--lite_version', default=False, type=bool,
                        help='model version of hopenet')
    parser.add_argument('--model_path', default='models/hopenet.pkl', type=str,
                        help='model path of hopenet : models/hopenet.pkl or models/update_model.pkl (lite version)')

    # face embedding model
    parser.add_argument('--model_file', default='MobileFace_Identification/MobileFace_Identification_V1', type=str,
                        help='model file of mobileface')
    parser.add_argument('--model_version', default='v1', type=str,
                        help='model version of mobileface : v1/v2/v3')
    parser.add_argument('--epoch', default=0, type=int)
    parser.add_argument('--batch_size', default=1, type=int)
    parser.add_argument('--database', default='models/database.pickle', type=str)

    # device
    parser.add_argument('--device', default='cpu', type=str,
                        help='cuda:0 or cpu')

    args = parser.parse_args()

    ret = {
        'server': {
            'ip': args.server_ip,
            'port': args.server_port,
        },
        'face_detection': {
            'net_type': args.net_type,
            'input_size': args.input_size,
            'threshold': args.threshold,
            'candidate_size': args.candidate_size,
            'device': args.device
        },
        'pose_estimation': {
            'lite_version': args.lite_version,
            'model_path': args.model_path,
            'batch_size': 1,
            'device': args.device,
        },
        'face_embedding': {
            'model_file': args.model_file,
            'model_version': args.model_version,
            'epoch': args.epoch,
            'batch_size': args.batch_size,
            'device': 'cpu',
            'database': args.database
        }
    }

    return ret
