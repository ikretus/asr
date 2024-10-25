import json
import os
import random
import subprocess
import sys
import time
import uuid

from conf import CONF, ROOT_DIR, SAMPLE_WAV, WHISPER
from datetime import datetime
from psycopg2 import Error, connect

SLEEP_TIME = 5.0
TODAY = datetime.now()


def wlog(vid, code, msg):
    level = {"i": "INFO", "w": "WARN", "e": "ERROR"}.get(code, "INFO")
    print("%s %s (%s) %s" % (datetime.now().strftime("%y%m%d:%H%M%S"), vid, level, msg), flush=True)


def gen_data(connector, n=8):
    dt = TODAY.strftime("%y%m%d")
    os.makedirs(dt, exist_ok=True)
    for i in range(n):
        auid = str(uuid.uuid4())
        lang = ("ru", "en")[random.randint(0, 1)]
        model = "lev%s" % random.randint(0, 4)
        create_auid(connector, auid, lang, model, "test")
        os.popen("cp %s %s/%s_%s_%s.wav" % (SAMPLE_WAV[lang], dt, auid, lang, model))
        os.wait()


def fext(fn, ext="wav"):
    return fn[:-3] + ext


def get_auid(fn):
    return fn.split("_", 1)[0][7:]


def get_model(fn):
    return os.path.join(WHISPER["model"]["path"], fext(fn.rsplit("_", 1)[1], "bin"))


def get_input(connector, mode):
    query = "SELECT auid, lang, model, loaded FROM " + CONF[mode]["table"] + " WHERE processing IS NULL ORDER BY loaded"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                inp = ["%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2]) for it in cur.fetchall()]
    except Error as err:
        wlog("db", "e", str(err).replace("\n", ". "))
        return list()
    n_inp = len(inp)
    inp = [it for it in inp if os.path.exists(it)]
    wlog("loaded", "i", "local = %s, total = %s" % (len(inp), n_inp))
    return inp if mode == "test" or len(inp) == 0 else [inp[0]]


def wait_proc(fn, proc, connector, mode):
    auid, status = get_auid(fn), proc.poll()
    if status is None:
        return True
    elif status == 0:
        wlog(auid, "i", "success")
        with open(fext(fn, "json")) as ff:
            update_status(connector, auid, "success", json.loads(ff.read()), mode)
    else:
        with open(fext(fn, "log")) as ff:
            wlog(auid, "e", ff.read().replace("\n", ". "))
        update_status(connector, auid, "failed", "whisper error", mode)
    return False


def run(cmds, connector, mode, it=0):
    ps = list()
    for cmd in cmds:
        auid = get_auid(cmd[it])
        ps.append((cmd[it], subprocess.Popen(cmd, stdout=open(fext(cmd[it], "log"), "w"), stderr=subprocess.STDOUT)))
        wlog(auid, "i", "processing")
        update_status(connector, auid, "processing", datetime.now(), mode)
        while len(ps) >= CONF["max_cpu"]:
            time.sleep(SLEEP_TIME)
            ps = [(fn, proc) for (fn, proc) in ps if wait_proc(fn, proc, connector, mode)]
    while len(ps) > 0:
        time.sleep(SLEEP_TIME)
        ps = [(fn, proc) for (fn, proc) in ps if wait_proc(fn, proc, connector, mode)]


def check_proc(connector, mode):
    ps, fn_pid = os.popen("ps --no-header -C %s -o pid,cmd" % WHISPER["exec"][0]).readlines(), dict()
    os.wait()
    for it in ps:
        it = it.split()
        if len(it) == 16:
            fn_pid[it[11]] = int(it[0])

    query = "SELECT auid, lang, model, loaded, processing, attempt FROM " + CONF[mode]["table"] + \
            " WHERE processing IS NOT NULL AND failed IS NULL AND success IS NULL"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                ps = cur.fetchall()
    except Error as err:
        wlog("db", "e", str(err).replace("\n", ". "))
        return None
    for it in ps:
        fn = "%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2])
        if os.path.exists(fn):
            ttl = 2 * (os.path.getsize(fn) * WHISPER["model"][CONF["n_thread"]][it[2]] / 32000.)
            if (TODAY - it[4]).total_seconds() > ttl:
                pid = fn_pid.pop(fn, None)
                if pid is not None:
                    try:
                        os.kill(pid, 9)
                        wlog(it[0], "w", "killed: processing too long")
                        update_status(connector, it[0], "failed", "killed", mode)
                    except Exception as err:
                        wlog("processing", "e", str(err).replace("\n", ". "))
                else:
                    if it[5] == 1:
                        wlog(it[0], "w", "resumed: 2nd attempt")
                        update_status(connector, it[0], "processing", None, mode)
                    else:
                        wlog(it[0], "w", "killed: 2nd attempt failed")
                        update_status(connector, it[0], "failed", "killed", mode)
    wlog("processing", "i", "local = %s, total = %s" % (len(fn_pid), len(ps)))
    return len(fn_pid) < CONF["max_cpu"]


def init_connector(conf, create_table=True):
    try:
        connector = connect(database=conf["database"], user=conf["user"], password=conf["password"],
                            host=conf["host"], port=conf["port"])
        connector.autocommit = True
    except Error as err:
        wlog("db", "e", str(err).replace("\n", ". "))
        return None
    if create_table:
        query = """
        CREATE TABLE IF NOT EXISTS %s (
            auid uuid NOT NULL PRIMARY KEY,
            lang char(2) NOT NULL,
            model char(4) NOT NULL,
            attempt smallint NOT NULL DEFAULT 0,
            loaded timestamp NOT NULL DEFAULT now(),
            processing timestamp, failed timestamp, success timestamp,
            log text, result jsonb, target jsonb
        ) """ % conf["table"]
        try:
            with connector:
                with connector.cursor() as cur:
                    cur.execute(query)
        except Error as err:
            wlog("db", "e", str(err).replace("\n", ". "))
            return None
    return connector


def create_auid(connector, auid, lang, model, mode="data"):
    try:
        with connector:
            with connector.cursor() as cur:
                query = "INSERT INTO " + CONF[mode]["table"] + "(auid, lang, model) VALUES (%s, %s, %s)"
                cur.execute(query, (auid, lang, model))
    except Error as err:
        wlog("db", "e", str(err).replace("\n", ". "))
        return False
    return True


def update_status(connector, auid, key, val, mode):
    table = CONF[mode]["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                if key == "processing":
                    query = "UPDATE " + table + " SET processing = %s, attempt = attempt + 1 WHERE auid = %s"
                    cur.execute(query, (val, auid))
                elif key == "failed":
                    query = "UPDATE " + table + " SET failed = %s, log = %s WHERE auid = %s"
                    cur.execute(query, (datetime.now(), val, auid))
                elif key == "success":
                    query = "UPDATE " + table + " SET success = %s, result = %s WHERE auid = %s"
                    cur.execute(query, (datetime.now(), json.dumps(val["transcription"]), auid))
                elif key == "target":
                    query = "UPDATE " + table + " SET target = %s WHERE auid = %s"
                    cur.execute(query, (json.dumps(val), auid))
    except Error as err:
        wlog(auid, "e", str(err).replace("\n", ". "))


if __name__ == "__main__":
    mode = "test" if len(sys.argv) == 2 and sys.argv[1].isdecimal() else "data"
    connector = init_connector(CONF[mode])
    if connector is None:
        sys.exit(1)

    root = os.path.join(ROOT_DIR, mode)
    os.makedirs(root, exist_ok=True)
    os.chdir(root)

    if mode == "test":
        gen_data(connector, int(sys.argv[1]))
    elif not check_proc(connector, mode):
        sys.exit(1)

    run([WHISPER["exec"] + [fn.rsplit("_", 2)[1], "-f", fn, "-m", get_model(fn), "-of", fn[:-4]]
         for fn in get_input(connector, mode)], connector, mode, 10)

    connector.close()
