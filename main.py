import conf
import json
import os
import random
import shutil
import subprocess
import sys
import time
import uuid

from datetime import datetime, timedelta
from glob import glob
from operator import itemgetter
from psycopg2 import Error, connect

TODAY = datetime.now()


def gen_data(connector=None):
    for i in range(conf.DAYS_TO_TEST):
        dt = (TODAY - timedelta(i)).strftime("%y%m%d")
        os.makedirs(dt, exist_ok=True)
        auid = str(uuid.uuid4())
        lang = ("ru", "en")[random.randint(0, 1)]
        model = "lev%s" % random.randint(0, 4)
        if connector is not None:
            if not create_auid(connector, auid, lang, model):
                print("AUID(%s) not created in DATABASE" % auid)
        os.popen("cp %s %s/%s_%s_%s.%s" % (conf.SAMPLE_WAV[lang], dt, auid, lang, model, "win"))
        os.wait()


def get_auid(fn):
    return fn.split("_", 1)[0][7:]


def duration(fn):
    m = fn.rsplit("_", 1)[1][:4]
    return os.path.getsize(fn) * conf.WHISPER["model"][conf.N_THREAD][m] / 32000.


def fext(fn, ext="wav"):
    return fn[:-3] + ext


def lang(fn):
    return fn.rsplit("_", 2)[1]


def model(fn):
    return os.path.join(conf.WHISPER["model"]["path"], fext(fn.rsplit("_", 1)[1], "bin"))


def input_files():
    inp = list()
    for i in (0, 1):
        os.makedirs((TODAY + timedelta(i)).strftime("%y%m%d"), exist_ok=True)
    for i in range(conf.DAYS_TO_SCAN):
        dt = (TODAY - timedelta(i)).strftime("%y%m%d")
        if os.path.exists(dt):
            inp.extend(glob(os.path.join(dt, "*.win")))
    return [it[0] for it in sorted([(fn, duration(fn)) for fn in inp], key=itemgetter(1))]


def run(cmds, connector=None, mode="test", it=0):
    def remove_finished(ps, connector=None, mode="test"):
        new_ps = list()
        for fn, proc in ps:
            auid, status = get_auid(fn), proc.poll()
            if status is None:
                new_ps.append((fn, proc))
            elif status == 0:
                print("DONE: %s" % fn, flush=True)
                os.rename(fn, fext(fn))
                if connector is not None:
                    if not update_status(connector, auid, "success", mode):
                        print("STATUS(%s) not updated to SUCCESS" % auid)
                    with open(fext(fn, "json")) as ff:
                        if not update_status(connector, auid, json.loads(ff.read()), mode):
                            print("JSON(%s) not loaded into DATABASE" % auid)
            else:
                print("FAIL: %s" % fn, flush=True)
                os.rename(fn, fext(fn, "failed"))
                if connector is not None:
                    if not update_status(connector, auid, "failed", mode):
                        print("STATUS(%s) not updated to FAILED" % auid)
                    with open(fext(fn, "log")) as ff:
                        if not update_status(connector, auid, ff.read(), mode):
                            print("LOG(%s) not loaded into DATABASE" % auid)
        return new_ps

    ps = list()
    for cmd in cmds:
        ps.append((cmd[it], subprocess.Popen(cmd, stdout=open(fext(cmd[it], "log"), "w"), stderr=subprocess.STDOUT)))
        if connector is not None:
            if not update_status(connector, get_auid(cmd[it]), "processing", mode):
                print("STATUS(%s) not updated to PROCESSING" % get_auid(cmd[it]))
        print("RUN: %s" % cmd[it], flush=True)
        while len(ps) >= conf.MAX_CPU:
            time.sleep(conf.SLEEP_TIME)
            ps = remove_finished(ps, connector)
    while len(ps) > 0:
        time.sleep(conf.SLEEP_TIME)
        ps = remove_finished(ps, connector)


def init_connector(conf, create_table=True):
    try:
        connector = connect(database=conf["database"], user=conf["user"], password=conf["password"],
                            host=conf["host"], port=conf["port"])
        connector.autocommit = True
    except Error as err:
        print(err)
        return None
    if create_table:
        query = """
        CREATE TABLE IF NOT EXISTS %s (
            auid uuid NOT NULL PRIMARY KEY,
            lang char(2) NOT NULL,
            model char(4) NOT NULL,
            loaded timestamp NOT NULL DEFAULT now(),
            processing timestamp, failed timestamp, success timestamp,
            log text, result jsonb, target jsonb
        ) """ % conf["table"]
        try:
            with connector:
                with connector.cursor() as cur:
                    cur.execute(query)
        except Error as err:
            print(err)
            return None
    return connector


def create_auid(connector, auid, lang, model, mode="test"):
    table = conf.DB_CONF[mode]["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                query = "INSERT INTO " + table + "(auid, lang, model) VALUES (%s, %s, %s)"
                cur.execute(query, (auid, lang, model))
    except Error as err:
        print(err)
        return False
    return True


def update_status(connector, auid, val, mode="test"):
    table = conf.DB_CONF[mode]["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                if isinstance(val, dict):
                    query = "UPDATE " + table + " SET result = %s WHERE auid = %s"
                    cur.execute(query, (json.dumps(val["transcription"]), auid))
                elif val in ("processing", "failed", "success"):
                    query = "UPDATE " + table + " SET " + val + " = %s WHERE auid = %s"
                    cur.execute(query, (datetime.now(), auid))
                else:
                    query = "UPDATE " + table + " SET log = %s WHERE auid = %s"
                    cur.execute(query, (val, auid))
    except Error as err:
        print(err)
        return False
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in conf.DB_CONF:
        print("USAGE: %s [test|data]" % sys.argv[0])
        sys.exit(1)

    mode, root = sys.argv[1], os.path.join(conf.ROOT_DIR, sys.argv[1])
    connector = init_connector(conf.DB_CONF[mode])

    if connector is None and mode != "test":
        print("EXIT: database connector unavailable in production mode")
        sys.exit(1)

    if mode == "test":
        shutil.rmtree(root, ignore_errors=True)

    os.makedirs(root, exist_ok=True)
    os.chdir(root)

    if mode == "test":
        gen_data(connector)

    inp = [conf.WHISPER["exec"] + [lang(fn), "-f", fn, "-m", model(fn), "-of", fn[:-4]] for fn in input_files()]
    if inp:
        print("input queue length = %s" % len(inp), flush=True)
        run(inp, connector, mode, 10)

    if connector is not None:
        connector.close()
