import json
import os
import psycopg2
import random
import shutil
import subprocess
import sys
import time

from conf import AUID_LEN, DAYS_TO_SCAN, DAYS_TO_TEST, DB_CONF, MAX_CPU, N_THREAD, ROOT_DIR, SAMPLE_WAV, TODAY, WHISPER
from datetime import datetime, timedelta
from glob import glob
from operator import itemgetter


def gen_data(days=8, connector=None):
    for i in range(days):
        dt = (TODAY - timedelta(i)).strftime("%y%m%d")
        os.makedirs(dt, exist_ok=True)
        auid = "%s/%s" % (dt, os.urandom(AUID_LEN).hex())
        lang = ("ru", "en")[random.randint(0, 1)]
        model = "lev%s" % random.randint(0, 4)
        if connector is not None:
            if not create_auid(connector, auid, lang, model):
                print("AUID(%s) not created in DATABASE" % auid)
        os.popen("cp %s %s_%s_%s.%s" % (SAMPLE_WAV[lang], auid, lang, model, "win"))
        os.wait()


def get_auid(fn):
    return fn.split("_", 1)[0]


def duration(fn):
    m = fn.rsplit("_", 1)[1][:4]
    return os.path.getsize(fn) * WHISPER["model"][N_THREAD][m] / 32000.


def fext(fn, ext="wav"):
    return fn[:-3] + ext


def lang(fn):
    return fn.rsplit("_", 2)[1]


def model(fn):
    return os.path.join(WHISPER["model"]["path"], fext(fn.rsplit("_", 1)[1], "bin"))


def input_files(days=8):
    inp = list()
    for i in (0, 1):
        os.makedirs((TODAY + timedelta(i)).strftime("%y%m%d"), exist_ok=True)
    for i in range(days):
        dt = (TODAY - timedelta(i)).strftime("%y%m%d")
        if os.path.exists(dt):
            inp.extend(glob(os.path.join(dt, "*.win")))
    return [it[0] for it in sorted([(fn, duration(fn)) for fn in inp], key=itemgetter(1))]


def run(cmds, it=0, sleep_time=1.0, connector=None, mode="test"):
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
        while len(ps) >= MAX_CPU:
            time.sleep(sleep_time)
            ps = remove_finished(ps, connector)
    while len(ps) > 0:
        time.sleep(sleep_time)
        ps = remove_finished(ps, connector)


def init_connector(conf):
    try:
        connector = psycopg2.connect(database=conf["database"], user=conf["user"], password=conf["password"],
                                     host=conf["host"], port=conf["port"])
        connector.autocommit = True
    except psycopg2.Error as err:
        print(err)
        return None
    query = """
    CREATE TABLE IF NOT EXISTS %s (
        auid char(27) NOT NULL PRIMARY KEY,
        lang char(2) NOT NULL,
        model char(4) NOT NULL,
        loaded timestamp, processing timestamp, failed timestamp, success timestamp,
        log text, result jsonb, target jsonb
    ) """ % conf["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
    except psycopg2.Error as err:
        print(err)
        return None
    return connector


def create_auid(connector, auid, lang, model, mode="test"):
    table = DB_CONF[mode]["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                query = "INSERT INTO " + table + "(auid, lang, model, loaded) VALUES (%s, %s, %s, %s)"
                cur.execute(query, (auid, lang, model, datetime.now()))
    except psycopg2.Error as err:
        print(err)
        return False
    return True


def update_status(connector, auid, val, mode="test"):
    table = DB_CONF[mode]["table"]
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
    except psycopg2.Error as err:
        print(err)
        return False
    return True


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in DB_CONF:
        print("USAGE: %s [test|data]" % sys.argv[0])
        sys.exit(1)

    mode, root = sys.argv[1], os.path.join(ROOT_DIR, sys.argv[1])
    connector = init_connector(DB_CONF[mode])

    if connector is None and mode != "test":
        print("EXIT: database connector unavailable in production mode")
        sys.exit(1)

    if mode == "test":
        shutil.rmtree(root, ignore_errors=True)

    os.makedirs(root, exist_ok=True)
    os.chdir(root)

    if mode == "test":
        gen_data(DAYS_TO_TEST, connector)

    inp = input_files(DAYS_TO_SCAN)
    if inp:
        print("input queue length = %s" % len(inp), flush=True)
        run([[WHISPER["exec"], "-p", "1", "-t", N_THREAD, "-ng", "-ojf", "-l", lang(fn), "-f", fn, "-m", model(fn),
              "-of", fn[:-4]] for fn in inp], 10, 0.5, connector, mode)

    if connector is not None:
        connector.close()
