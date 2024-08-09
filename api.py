import base64
import conf
import os
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
    except:
        abort(415)
    auid = str(uuid.uuid4())
    wav = "%s_%s_%s.win" % (auid, lang, model)
    with open(auid, "wb") as ff:
        ff.write(data)

    connector = init_connector(conf.DB_CONF[MODE], create_table=False)
    cmd = "ffmpeg -hide_banner -v error -i %s -ar 16000 -ac 1 -c:a pcm_s16le %s 2>&1"
    with os.popen(cmd % (auid, fext(wav))) as pipe:
        if os.wait()[1] == 0:
            root = os.path.join(conf.ROOT_DIR, MODE, TODAY.strftime("%y%m%d"))
            os.makedirs(root, exist_ok=True)
            os.rename(fext(wav), os.path.join(root, wav))
            status, code = {"auid": auid}, 202
            if connector is not None:
                if create_auid(connector, auid, lang, model):
                    status.update({"status": "loaded"})
        else:
            status, code = {"error": "[FFMPEG] %s" % pipe.read()}, 415
    os.remove(auid)
    if connector is not None:
        connector.close()
    return make_response(status, code)


@app.get("/<uuid:auid>")
def get_status_or_result(auid=None):
    auid, db_conf = escape(auid), conf.DB_CONF[MODE]
    connector = init_connector(db_conf, create_table=False)
    query = "SELECT loaded, processing, failed, log, result FROM " + db_conf["table"] + " WHERE auid = %s"
    try:
        with connector:
            with connector.cursor() as cur:
                cur.execute(query, [auid])
                data = cur.fetchone()
    except Error:
        data = None
    if connector is not None:
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


if __name__ == "__main__":
    app.run(debug=True)
