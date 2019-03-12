import sys

from prediction import Prediction


def main(data_to_predict_json):
    print('### Delivery Time Prediction App ###')
    pred = Prediction(data_to_predict_json)
    pred.predict()


if __name__ == "__main__":
    print('Number of arguments:', len(sys.argv))
    print('Argument List:', str(sys.argv))
    if len(sys.argv) < 2:
        print('[ERROR] Please pass a valid json file')
        sys.exit(1)
    main(sys.argv[1])

