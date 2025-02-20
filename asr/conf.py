import json
import os

DATA_DIR = os.path.expanduser("~/devel/asr/data")

with open(os.path.expanduser("~/devel/asr/conf.json")) as ff:
    CONF = json.loads(ff.read())

SAMPLE_WAV = {
    "en": os.path.join(os.path.expanduser(CONF["sample_dir"]), "sample_en.wav"),
    "ru": os.path.join(os.path.expanduser(CONF["sample_dir"]), "sample_ru.wav")
}

ojf = "-ojf" if CONF["output_json_full"] else "-oj"
WHISPER = {
    "exec": [CONF["whisper"], "-p", str(CONF["n_proc"]), "-t", str(CONF["n_thread"]), "-ng", ojf, "-l"],
    "model": {
        "path": os.path.expanduser(CONF["model_dir"]),
        1: {"lev0": 0.15, "lev1": 0.19, "lev2": 0.92, "lev3": 2.1, "lev4": 3.9},
        2: {"lev0": 0.18, "lev1": 0.25, "lev2": 0.59, "lev3": 1.4, "lev4": 2.5},
    }
}
