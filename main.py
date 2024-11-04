import json
import os
import random
import subprocess
import sys
import time
import uuid

from conf import CONF, DATA_DIR, SAMPLE_WAV, WHISPER
from datetime import datetime
from psycopg2 import Error, connect


def wlog(vid, code, msg):
    level = {"i": "INFO", "w": "WARN", "e": "ERRO"}.get(code, "INFO")
    print("%s %s (%s) %s" % (datetime.now().strftime("%y%m%d:%H%M%S"), level, vid, msg), flush=True)


def whisper(fn, path=None):
    m = os.path.join(WHISPER["model"]["path"], fext(fn.rsplit("_", 1)[1], "bin"))
    cmd = WHISPER["exec"] + [fn.rsplit("_", 2)[1], "-f", fn, "-m", m, "-of", fn[:-4]]
    if path is not None:
        cmd[0] = path
    return cmd


def make_test_env(connector, n):
    fns = list()
    for i in range(n):
        auid = str(uuid.uuid4())
        lang = ("ru", "en")[random.randint(0, 1)]
        model = "lev%s" % random.randint(0, 4)
        if create_auid(connector, auid, lang, model):
            dt = datetime.now().strftime("%y%m%d")
            os.makedirs(dt, exist_ok=True)
            fns.append("%s/%s_%s_%s.wav" % (dt, auid, lang, model))
            os.popen("cp %s %s" % (SAMPLE_WAV[lang], fns[-1]))
            os.wait()

    fn, fn2 = random.sample(fns, 2)
    update_status(connector, get_auid(fn2))
    cmd = os.path.join(DATA_DIR, "whisper")
    if os.path.exists(cmd):
        update_status(connector, get_auid(fn))
        subprocess.Popen(whisper(fn, cmd), stdout=open(fext(fn, "log"), "w"), stderr=subprocess.STDOUT)


def fext(fn, ext="wav"):
    return fn[:-3] + ext


def get_auid(fn):
    return fn.split("_", 1)[0][7:]


def get_input(connector):
    query = "SELECT auid, lang, model, loaded FROM " + CONF["table"] + " WHERE processing IS NULL ORDER BY loaded"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                inp = ["%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2]) for it in cur.fetchall()]
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
        return list()
    return [it for it in inp if os.path.exists(it) and os.path.getsize(it) > CONF["wav_min_size"]]


def wait_proc(fn, proc, connector):
    auid, status = get_auid(fn), proc.poll()
    if status is None:
        return True
    elif status == 0:
        try:
            with open(fext(fn, "json")) as ff:
                update_status(connector, auid, json.loads(ff.read()))
            wlog(auid, "i", "success")
        except FileNotFoundError:
            update_status(connector, auid, "error:whisper")
            wlog(auid, "e", ".json not found")
    else:
        update_status(connector, auid, "error:whisper")
        try:
            with open(fext(fn, "log")) as ff:
                wlog(auid, "e", " ".join(ff.read().split("\n")[:5]))
        except FileNotFoundError:
            wlog(auid, "e", ".log not found")
    return False


def run(inp, connector, sleep=5.0):
    ps = list()
    for fn, cmd in inp:
        auid = get_auid(fn)
        ps.append((fn, subprocess.Popen(cmd, stdout=open(fext(fn, "log"), "w"), stderr=subprocess.STDOUT)))
        wlog(auid, "i", "processing")
        update_status(connector, auid)
    while len(ps) > 0:
        time.sleep(sleep)
        ps = [(fn, proc) for (fn, proc) in ps if wait_proc(fn, proc, connector)]


def check_proc(connector):
    query = "SELECT auid, lang, model, loaded, processing, attempt FROM " + CONF["table"] + \
            " WHERE processing IS NOT NULL AND failed IS NULL AND success IS NULL"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                proc = cur.fetchall()
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
        return 0
    if not proc:
        return CONF["max_cpu"]

    ps, fn_pid = os.popen("ps --no-headers --cols 1000 -C whisper -o pid,cmd").readlines(), dict()
    os.wait()
    for it in ps:
        it = it.strip().split()
        if len(it) >= 16:
            fn_pid["%s.wav" % it[-1]] = int(it[0])

    for it in proc:
        fn = "%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2])
        if os.path.exists(fn):
            ttl = CONF["ttl"] * os.path.getsize(fn) * WHISPER["model"][CONF["n_thread"]][it[2]] / 32000.
            if (datetime.now() - it[4]).total_seconds() > ttl:
                pid = fn_pid.pop(fn, None)
                if pid is not None:
                    try:
                        os.kill(pid, 9)
                        wlog(it[0], "w", "killed")
                        update_status(connector, it[0], "killed:toolong")
                    except Exception as err:
                        wlog("task", "e", str(err).replace("\n", " "))
                else:
                    if it[5] == 1:
                        wlog(it[0], "w", "attempt resumed")
                        update_status(connector, it[0], 0)
                    else:
                        wlog(it[0], "w", "attempt failed")
                        update_status(connector, it[0], "failed:attempt")
    return CONF["max_cpu"] - len(fn_pid)


def init_connector(create_table=False):
    try:
        connector = connect(database=CONF["database"], user=CONF["user"], password=CONF["password"],
                            host=CONF["host"], port=CONF["port"])
        connector.autocommit = True
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
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
        ) """ % CONF["table"]
        try:
            with connector:
                with connector.cursor() as cur:
                    cur.execute(query)
        except Error as err:
            wlog("db", "e", str(err).replace("\n", " "))
            connector.close()
    return connector


def create_auid(connector, auid, lang, model):
    try:
        with connector:
            with connector.cursor() as cur:
                query = "INSERT INTO " + CONF["table"] + "(auid, lang, model) VALUES (%s, %s, %s)"
                cur.execute(query, (auid, lang, model))
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
        return False
    return True


def update_status(connector, auid, val=None):
    table = CONF["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                if val is None:
                    query = "UPDATE " + table + " SET processing = now(), attempt = attempt + 1 WHERE auid = %s"
                    cur.execute(query, [auid])
                elif val == 0:
                    query = "UPDATE " + table + " SET processing = NULL WHERE auid = %s"
                    cur.execute(query, [auid])
                elif isinstance(val, dict):
                    query = "UPDATE " + table + " SET success = now(), log = %s, result = %s WHERE auid = %s"
                    cur.execute(query, ("success", json.dumps(val["transcription"]), auid))
                else:
                    query = "UPDATE " + table + " SET failed = now(), log = %s WHERE auid = %s"
                    cur.execute(query, (val, auid))
    except Error as err:
        wlog(auid, "e", str(err).replace("\n", " "))


if __name__ == "__main__":
    connector = init_connector(CONF["create_table"])
    if connector is not None:
        os.chdir(DATA_DIR)
        if len(sys.argv) == 2 and sys.argv[1].isdecimal():
            make_test_env(connector, int(sys.argv[1]))
        else:
            n = check_proc(connector)
            if n > 0:
                inp = get_input(connector)
                if inp:
                    wlog("task", "i", "check = %s, input = %s" % (n, len(inp)))
                    run([(fn, whisper(fn)) for fn in inp[:n]], connector)
        connector.close()
