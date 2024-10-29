import base64
import os
import shutil
import uuid

from conf import CONF, DATA_DIR, SAMPLE_WAV, WHISPER
from datetime import datetime
from flask import Flask, abort, make_response, request
from markupsafe import escape
from main import Error, init_connector, create_auid

FETCH_MANY = 100
app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False


@app.errorhandler(404)
def not_found(err):
    return make_response({"error": "auid not found"}, 404)


@app.post("/")
def load_audio():
    lang, model = request.json.get("lang"), request.json.get("model")
    if lang not in SAMPLE_WAV or model not in WHISPER["model"][2] or "data" not in request.json:
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

    connector = init_connector(CONF)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    wav = "%s_%s_%s.wav" % (auid, lang, model)
    cmd = "ffmpeg -hide_banner -v error -i %s -ar 16000 -ac 1 -c:a pcm_s16le %s 2>&1"
    with os.popen(cmd % (auid, wav)) as pipe:
        if os.wait()[1] == 0:
            if create_auid(connector, auid, lang, model):
                root = os.path.join(DATA_DIR, datetime.now().strftime("%y%m%d"))
                os.makedirs(root, exist_ok=True)
                shutil.move(wav, os.path.join(root, wav))
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
    auid, connector = escape(auid), init_connector(CONF, False)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    query = "SELECT loaded, processing, failed, log, result FROM " + CONF["table"] + " WHERE auid = %s"
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
    res, connector = list(), init_connector(CONF, False)
    if connector is None:
        return make_response({"error": "[DB] connector unavailable"}, 500)

    query = "SELECT auid, loaded, processing, failed, success FROM " + CONF["table"] + " ORDER BY loaded DESC"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query)
                data = cur.fetchmany(FETCH_MANY)
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
