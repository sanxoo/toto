from typer import Typer, Argument, confirm, Abort
from typing_extensions import Annotated
from pathlib import Path
from uuid import UUID
from typing import List
from rich import print

import importlib.util
import json

from flows.share import db

work = Typer(help="toto... work flow management cli")

@work.command("add", help="add new work flow")
def work_add(
    name: Annotated[str, Argument(help="name of the work flow")],
    flow: Annotated[str, Argument(help="flow of the work flow, module.flow")],
    spec_file: Annotated[Path, Argument(help="json format spec file name")]
):
    work = db.insert(name, flow, json.dumps(json.load(spec_file.open())))
    print(work)

@work.command("ls", help="list all work flows")
def work_ls():
    for work in db.search():
        item = {
            "work_id:name:flow:tags": f"{work['work_id']}:{work['name']}:{work['flow']}:{work['tags']}",
            "spec": work["spec"],
            "work_queue_name:cron:deployment_id": f"{work['work_queue_name']}:{work['cron']}:{work['deployment_id']}",
            "created_at:updated_at": f"{work['created_at']}:{work['updated_at']}"
        }
        print(item)

@work.command("show", help="show details of the work flow")
def work_show(
    work_id: Annotated[UUID, Argument(help="id of the work flow")]
):
    work = db.select(work_id)
    print(work)

@work.command("rename", help="update the name of the work flow")
def work_rename(
    work_id: Annotated[UUID, Argument(help="id of the work flow")],
    name: Annotated[str, Argument(help="new name of the work flow")]
):
    part = {
        "name": name
    }
    work = db.update(work_id, part)
    print(work)

@work.command("spec", help="update the spec of the work flow")
def work_spec(
    work_id: Annotated[UUID, Argument(help="id of the work flow")],
    spec_file: Annotated[Path, Argument(help="json format spec file name")]
):
    part = {
        "spec": json.dumps(json.load(spec_file.open()))
    }
    work = db.update(work_id, part)
    print(work)

@work.command("tags", help="update tags of the work flow")
def work_tags(
    work_id: Annotated[UUID, Argument(help="id of the work flow")],
    tags: Annotated[List[str], Argument(help="id of the work flow")] = None
):
    part = {
        "tags": ",".join(tags)
    }
    work = db.update(work_id, part)
    print(work)

@work.command("rm", help="remove the work flow from database")
def work_rm(
    work_id: Annotated[UUID, Argument(help="id of the work flow")]
):
    if not confirm("Are you sure?"): raise Abort()
    db.delete(work_id)

@work.command("test", help="test spec of the work flow")
def work_test(
    work_id: Annotated[UUID, Argument(help="id of the work flow")]
):
    work = db.select(work_id)
    module_name = f"flows.{work['flow'].split('.')[0]}"
    module_spec = importlib.util.find_spec(module_name)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)
    res = getattr(module, "test")(work_id)
    print(res)

@work.command("run", help="deploy the work flow to run")
def work_run(
    work_id: Annotated[UUID, Argument(help="id of the work flow")],
    work_queue_name: Annotated[str, Argument(help="name of the prefect's work queue to run")],
    cron: Annotated[str, Argument(help="crontab style schedule, '0 0 * * *'")]
):
    ...

@work.command("stop", help="undeploy the work flow to stop")
def work_stop(
    work_id: Annotated[UUID, Argument(help="id of the work flow")]
):
    ...

if __name__ == "__main__":
    work(prog_name="toto")

