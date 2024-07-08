from prefect import flow, task

import xml.etree.ElementTree
import json
import requests

from .share import ev#, dh

def parse(text):
    rows = []
    for item in xml.etree.ElementTree.fromstring(text).iterfind("body/items/item"):
        rows.append({e.tag: e.text for e in item})
    return rows

@task
def extract(spec):
    url, params = spec["url_or_query"], spec["params"]
    if "pageNo" in params: params["pageNo"] = 1
    data = []
    while 1:
        response = requests.get(url, params=params)
        rows = parse(response.text)
        data = data + rows
        if "pageNo" not in params or len(rows) < params["numOfRows"]: break
        params["pageNo"] += 1
    return data

def test(work_id):
    spec = ev.spec.fn(work_id)
    url, params = spec["url_or_query"], spec["params"]
    response = requests.get(url, params=params)
    res = {
        "code": response.status_code, "text": response.text
    }
    try:
        res["data"] = parse(response.text)
    except:
        res["data"] = []
    return res

@flow
def collect(work_id):
    spec = ev.spec(work_id)
    data = extract(spec)
    #dh.upload(spec["identifier"], data)

