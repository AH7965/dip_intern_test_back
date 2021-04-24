from flask import Flask, abort, request, jsonify

from queue import Queue
import functools
import pandas as pd
import time

import lightgbm as lgb

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
def slack_notify():
    if request.args.get('device_name') is not None:
        device_name = request.args.get('device_name')
    else:
        device_name = "Unknown Device"

    if request.args.get('project_name') is not None:
        project_name = request.args.get('project_name')
    else:
        project_name = "Unknown Project"

    if request.args.get('text') is not None:
        text = request.args.get('text')
    else:
        text = "No text"

    sentence = f"[NOTIFY] \n{device_name} : \n\t{project_name} : \n\t\t{text}"


    slack.notify(text=sentence)

    return jsonify({"text" : sentence})





if __name__ == "__main__":
    app.run(debug=True)