from datetime import date, datetime, timedelta

from prefect import task

import json

from . import db

def eval_str(v):
    return eval(v[4:]) if v.lower().startswith("%py,") else v

def evaluate(d):
    for k, v in d.items():
        if type(v) is dict: d[k] = evaluate(v)
        if type(v) is str : d[k] = eval_str(v)
    return d

@task
def spec(work_id):
    work = db.select(work_id)
    d = json.loads(work["spec"])
    return evaluate(d)

