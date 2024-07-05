import pymysql
import uuid

_params = {
    "host": "192.168.0.97",
    "port": 3306,
    "user": "emma",
    "password": "1dls1ekfr",
    "database": "elt"
}

def search():
    with pymysql.connect(**_params) as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            query = (
                "select * from works order by name "
            )
            cursor.execute(query)
            return cursor.fetchall()

def select(work_id):
    with pymysql.connect(**_params) as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            query = (
                f"select * from works where work_id = '{work_id}' "
            )
            cursor.execute(query)
            work = cursor.fetchone()
            if not work: raise LookupError(f"not found for work {work_id}")
            return work

def insert(name, flow, spec):
    with pymysql.connect(**_params) as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            work_id = str(uuid.uuid4())
            query = (
                f"insert into works (work_id, name, flow, spec) values ('{work_id}', '{name}', '{flow}', '{spec}') "
            )
            cursor.execute(query)
            conn.commit()
            query = (
                f"select * from works where work_id = '{work_id}' "
            )
            cursor.execute(query)
            return cursor.fetchone()

def update(work_id, part):
    with pymysql.connect(**_params) as conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            query = (
                f"select * from works where work_id = '{work_id}' "
            )
            cursor.execute(query)
            work = cursor.fetchone()
            if not work: raise LookupError(f"not found for work {work_id}")
            work.update(part)
            query = (
                "update works set name = %(name)s, spec = %(spec)s, tags = %(tags)s, "
                "work_queue_name = %(work_queue_name)s, cron = %(cron)s, "
                "deployment_id = %(deployment_id)s "
                "where work_id = %(work_id)s "
            )
            cursor.execute(query, work)
            conn.commit()
            query = (
                f"select * from works where work_id = '{work_id}' "
            )
            cursor.execute(query)
            return cursor.fetchone()

def delete(work_id):
    with pymysql.connect(**_params) as conn:
        with conn.cursor() as cursor:
            query = (
                f"delete from works where work_id = '{work_id}' "
            )
            cursor.execute(query)
            conn.commit()


if __name__ == "__main__":
    with pymysql.connect(**_params) as conn:
        with conn.cursor() as cursor:
            query = (
                "create table if not exists works ( "
                "    work_id         uuid not null primary key, "
                "    name            tinytext not null, "
                "    flow            tinytext not null, "
                "    spec            json not null check (json_valid(spec)), "
                "    tags            tinytext, "
                "    work_queue_name tinytext, "
                "    cron            tinytext, "
                "    deployment_id   uuid, "
                "    created_at      timestamp not null default current_timestamp, "
                "    updated_at      timestamp on update current_timestamp "
                ") "
            )
            cursor.execute(query)

