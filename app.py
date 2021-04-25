from flask import Flask, abort, request, Response

from queue import Queue
import functools
import time
from resister import slack_token
from parameter import *
from preprocess import preprocess
from estimate import estimate
from postprocess import postprocess
from io import StringIO
import slackweb
import pandas as pd
import os.path as osp


slack = slackweb.Slack(f"https://hooks.slack.com/services/{slack_token}")


app = Flask(__name__)



singleQueue = Queue(maxsize=1)

def multiple_control(q):
    def _multiple_control(func):
        @functools.wraps(func)
        def wrapper(*args,**kwargs):
            q.put(time.time())
            print("/// [start] critial zone")
            result = func(*args,**kwargs)
            print("/// [end] critial zone")
            time.sleep(0.5)
            q.get()
            q.task_done()
            return result
        return wrapper
    return _multiple_control

def heavy_process(data):
    print("<business> : " + data)
    time.sleep(10)
    return "ok : " + data


@multiple_control(singleQueue)
@app.route('/')
def hello():
    if request.headers.getlist("X-Forwarded-For"):
        ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
        ip = request.remote_addr
    slack.notify(text=f"[ACCESSED ROOT] : \n from {ip}")
    return abort(404)

@multiple_control(singleQueue)
@app.route('/estimate', methods=["POST"])
def app_estimate():
    test_csv = request.files.get('file')
    if test_csv is None:
        return abort(404)
    if request.headers.getlist("X-Forwarded-For"):
        ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
        ip = request.remote_addr
    slack.notify(text=f"[ESTIMATE] : \n from {ip}")

    if isinstance(test_csv, FileStorage) and test_csv.content_type == 'text/csv':
        test_df = pd.read_csv(test_csv)
    else:
        return abort(404, {'code': 'Wrong file', 'message': 'the file is not csv please send me csv'})
    
    test_encoded = preprocess(test_df)
    test_preds_df = estimate(test_encoded)
    submit_df = postprocess(test_preds_df, test_df)
    test_df.to_csv(osp.join(DATA_DIR, f'dip_input_{time.time()}.csv'), index=False, header=True)
    submit_df.to_csv(osp.join(OUTPUT_DIR, f'dip_estimete_{time.time()}.csv'), index=False, header=True)

    textStream = StringIO()
    submit_df.to_csv(textStream, index=False, header=True)
    output_name = f"estimate_{test_csv.filename}"
    if output_name[-4:] != ".csv":
        output_name += ".csv"

    return Response(
        textStream.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition":
                f"attachment; filename={output_name}"})


@multiple_control(singleQueue)
@app.route('/estimate_test', methods=["GET"])
def app_estimate_test():
    if request.headers.getlist("X-Forwarded-For"):
        ip = request.headers.getlist("X-Forwarded-For")[0]
    else:
        ip = request.remote_addr
    slack.notify(text=f"[ESTIMATE TEST] : \n from {ip}")


    test_df = pd.read_csv('./input/test_x.csv')
    
    test_encoded = preprocess(test_df)
    test_preds_df = estimate(test_encoded)
    submit_df = postprocess(test_preds_df, test_df)
    test_df.to_csv(osp.join(DATA_DIR, f'dip_input_{time.time()}.csv'), index=False, header=True)
    submit_df.to_csv(osp.join(OUTPUT_DIR, f'dip_estimete_{time.time()}.csv'), index=False, header=True)

    textStream = StringIO()
    submit_df.to_csv(textStream, index=False, header=True)

    return Response(
        textStream.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition":
                "attachment; filename=estimate.csv"})



if __name__ == "__main__":
    app.run(debug=True)