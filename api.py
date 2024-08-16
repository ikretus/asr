import base64
import conf
import os
import shutil
import uuid

from flask import Flask, abort, make_response, request
from markupsafe import escape
from main import Error, TODAY, fext, init_connector, create_auid

MODE = "data"
app = Flask(__name__)


@app.errorhandler(404)
def not_found(err):
    return make_response({"error": "auid not found"}, 404)


@app.post("/")
def load_audio():
    lang, model = request.json.get("lang"), request.json.get("model")
    if lang not in conf.SAMPLE_WAV or model not in conf.WHISPER["model"]["2"] or "data" not in request.json:
        abort(400)
    try:
        data = base64.b64decode(request.json["data"])
    except Exception:
        abort(415)
    auid = str(uuid.uuid4())
    try:
        with open(auid, "wb") as ff:
            ff.write(data)
    except Exception as err:
        return make_response({"error": "[SYS] %s" % str(err)}, 500)

    connector = init_connector(conf.DB_CONF[MODE], create_table=False)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    wav = "%s_%s_%s.wav" % (auid, lang, model)
    cmd = "ffmpeg -hide_banner -v error -i %s -ar 16000 -ac 1 -c:a pcm_s16le %s 2>&1"
    with os.popen(cmd % (auid, wav)) as pipe:
        if os.wait()[1] == 0:
            root = os.path.join(conf.ROOT_DIR, MODE, TODAY.strftime("%y%m%d"))
            win = os.path.join(root, fext(wav, "win"))
            if create_auid(connector, auid, lang, model):
                os.makedirs(root, exist_ok=True)
                try:
                    os.rename(wav, win)
                except Exception:
                    shutil.move(wav, win[:-4])
                    os.rename(win[:-4], win)
                resp = make_response({"auid": auid, "status": "loaded"}, 202)
            else:
                os.remove(wav)
                resp = make_response({"error": "[DB] auid not created"}, 500)
        else:
            resp = make_response({"error": "[FFMPEG] %s" % pipe.read()}, 415)
    os.remove(auid)
    connector.close()
    return resp


@app.get("/<uuid:auid>")
def get_status_or_result(auid=None):
    auid, db_conf = escape(auid), conf.DB_CONF[MODE]
    connector = init_connector(db_conf, create_table=False)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    query = "SELECT loaded, processing, failed, log, result FROM " + db_conf["table"] + " WHERE auid = %s"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query, [auid])
                data = cur.fetchone()
    except Error:
        data = None
    connector.close()
    if data is None or not data[0]:
        abort(404)
    if data[4]:
        return {"auid": auid, "status": "success", "result": data[4]}
    if data[2]:
        return make_response({"auid": auid, "status": "failed", "log": data[3]}, 500)
    if data[1]:
        return {"auid": auid, "status": "processing"}
    return {"auid": auid, "status": "loaded"}


@app.get("/")
def get_multi_status():
    db_conf, res = conf.DB_CONF[MODE], list()
    connector = init_connector(db_conf, create_table=False)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    query = "SELECT auid, loaded, processing, failed, success FROM " + db_conf["table"] + " ORDER BY loaded DESC"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                data = cur.fetchmany(conf.FETCH_MANY)
    except Error:
        data = None
    connector.close()
    if data is None:
        return make_response({"error": "no data"}, 404)
    for it in data[::-1]:
        if it[4]:
            status = "success"
        elif it[3]:
            status = "failed"
        elif it[2]:
            status = "processing"
        else:
            status = "loaded"
        res.append({"auid": it[0], "status": status})
    return res


if __name__ == "__main__":
    app.run(debug=True)
