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
    level = {"i": "INFO", "w": "WARN", "e": "ERROR"}.get(code, "INFO")
    print("%s %s (%s) %s" % (datetime.now().strftime("%y%m%d:%H%M%S"), vid, level, msg), flush=True)


def make_test_env(connector, n):
    dt, fns = datetime.now().strftime("%y%m%d"), list()
    os.makedirs(dt, exist_ok=True)
    for i in range(n):
        auid = str(uuid.uuid4())
        lang = ("ru", "en")[random.randint(0, 1)]
        model = "lev%s" % random.randint(0, 4)
        create_auid(connector, auid, lang, model)
        fns.append("%s/%s_%s_%s.wav" % (dt, auid, lang, model))
        os.popen("cp %s %s" % (SAMPLE_WAV[lang], fns[-1]))
        os.wait()

    fn, fn2 = random.sample(fns, 2)
    update_status(connector, get_auid(fn), "processing", datetime.now())
    update_status(connector, get_auid(fn2), "processing", datetime.now())

    cmd = WHISPER["exec"] + [fn.rsplit("_", 2)[1], "-f", fn, "-m", get_model(fn), "-of", fn[:-4]]
    cmd[0] = "./" + cmd[0]
    subprocess.Popen(cmd, stdout=open(fext(cmd[10], "log"), "w"), stderr=subprocess.STDOUT)


def fext(fn, ext="wav"):
    return fn[:-3] + ext


def get_auid(fn):
    return fn.split("_", 1)[0][7:]


def get_model(fn):
    return os.path.join(WHISPER["model"]["path"], fext(fn.rsplit("_", 1)[1], "bin"))


def get_input(connector):
    query = "SELECT auid, lang, model, loaded FROM " + CONF["table"] + " WHERE processing IS NULL ORDER BY loaded"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                inp = ["%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2]) for it in cur.fetchall()]
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
        return None
    n_inp = len(inp)
    inp = [it for it in inp if os.path.exists(it)]
    wlog("loaded", "i", "local = %s, total = %s" % (len(inp), n_inp))
    return inp


def wait_proc(fn, proc, connector):
    auid, status = get_auid(fn), proc.poll()
    if status is None:
        return True
    elif status == 0:
        wlog(auid, "i", "success")
        with open(fext(fn, "json")) as ff:
            update_status(connector, auid, "success", json.loads(ff.read()))
    else:
        with open(fext(fn, "log")) as ff:
            wlog(auid, "e", " ".join(ff.read().split("\n")[:5]))
        update_status(connector, auid, "failed", "error:whisper")
    return False


def run(cmds, connector, it=0, sleep=5.0):
    ps = list()
    for cmd in cmds:
        auid = get_auid(cmd[it])
        ps.append((cmd[it], subprocess.Popen(cmd, stdout=open(fext(cmd[it], "log"), "w"), stderr=subprocess.STDOUT)))
        wlog(auid, "i", "processing")
        update_status(connector, auid, "processing", datetime.now())
    while len(ps) > 0:
        time.sleep(sleep)
        ps = [(fn, proc) for (fn, proc) in ps if wait_proc(fn, proc, connector)]


def check_proc(connector):
    ps, fn_pid = os.popen("ps --no-headers --cols 1000 -C whisper -o pid,cmd").readlines(), dict()
    os.wait()
    for it in ps:
        it = it.split()
        if len(it) >= 16:
            fn_pid["%s.wav" % it[-1]] = int(it[0])

    query = "SELECT auid, lang, model, loaded, processing, attempt FROM " + CONF["table"] + \
            " WHERE processing IS NOT NULL AND failed IS NULL AND success IS NULL"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                ps = cur.fetchall()
    except Error as err:
        wlog("db", "e", str(err).replace("\n", " "))
        return 0

    o_proc = len(ps) - len(fn_pid)
    for it in ps:
        fn = "%s/%s_%s_%s.wav" % (it[3].strftime("%y%m%d"), it[0], it[1], it[2])
        if os.path.exists(fn):
            ttl = 2 * (os.path.getsize(fn) * WHISPER["model"][CONF["n_thread"]][it[2]] / 32000.)
            if (datetime.now() - it[4]).total_seconds() > ttl:
                pid = fn_pid.pop(fn, None)
                if pid is not None:
                    try:
                        os.kill(pid, 9)
                        wlog(it[0], "w", "killed: processing too long")
                        update_status(connector, it[0], "failed", "killed:toolong")
                    except Exception as err:
                        wlog("processing", "e", str(err).replace("\n", " "))
                else:
                    if it[5] == 1:
                        wlog(it[0], "w", "resumed: attempt started")
                        update_status(connector, it[0], "processing", None)
                    else:
                        wlog(it[0], "w", "killed: repeated attempt failed")
                        update_status(connector, it[0], "failed", "killed:repeated")
    wlog("processing", "i", "local = %s of %s, total = %s" % (len(fn_pid), CONF["max_cpu"], o_proc + len(fn_pid)))
    return CONF["max_cpu"] - len(fn_pid)


def init_connector(conf, create_table=True):
    try:
        connector = connect(database=conf["database"], user=conf["user"], password=conf["password"],
                            host=conf["host"], port=conf["port"])
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
        ) """ % conf["table"]
        try:
            with connector:
                with connector.cursor() as cur:
                    cur.execute(query)
        except Error as err:
            wlog("db", "e", str(err).replace("\n", " "))
            return None
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


def update_status(connector, auid, key, val):
    table = CONF["table"]
    try:
        with connector:
            with connector.cursor() as cur:
                if key == "processing":
                    query = "UPDATE " + table + " SET processing = %s, attempt = attempt + 1 WHERE auid = %s"
                    cur.execute(query, (val, auid))
                elif key == "failed":
                    query = "UPDATE " + table + " SET failed = now(), log = %s WHERE auid = %s"
                    cur.execute(query, (val, auid))
                elif key == "success":
                    query = "UPDATE " + table + " SET success = now(), log = %s, result = %s WHERE auid = %s"
                    cur.execute(query, ("success", json.dumps(val["transcription"]), auid))
                elif key == "target":
                    query = "UPDATE " + table + " SET target = %s WHERE auid = %s"
                    cur.execute(query, (json.dumps(val), auid))
    except Error as err:
        wlog(auid, "e", str(err).replace("\n", " "))


if __name__ == "__main__":
    connector = init_connector(CONF)
    if connector is None:
        sys.exit(1)

    os.chdir(DATA_DIR)
    if len(sys.argv) == 2 and sys.argv[1].isdecimal():
        make_test_env(connector, int(sys.argv[1]))
    else:
        n = check_proc(connector)
        if n < 1:
            sys.exit(0)
        run([WHISPER["exec"] + [fn.rsplit("_", 2)[1], "-f", fn, "-m", get_model(fn), "-of", fn[:-4]]
             for fn in get_input(connector)[:n]], connector, 10)

    connector.close()
