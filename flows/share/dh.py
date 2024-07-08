from prefect import task

import pyiceberg.catalog
import pyarrow as pa

_params = {
    "uri": "http://192.168.0.97:8181",
    "s3.endpoint": "http://192.168.0.97:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin"
}

@task
def upload(identifier, data):
    catalog = pyiceberg.catalog.load_catalog("default", **_params)
    dt = pa.Table.from_pylist(data)
    try:
        table = catalog.load_table(identifier)
    except:
        ns, _ = identifier.split(".")
        if tuple(ns) not in catalog.list_namespaces(): catalog.create_namespace(ns)
        table = catalog.create_table(identifier, schema=dt.schema)
    table.append(dt)

