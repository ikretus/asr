import json
import os

ROOT_DIR: str = os.path.join(os.environ["HOME"], "devel/asr")

with open(os.path.join(ROOT_DIR, "conf.json")) as ff:
    CONF = json.loads(ff.read())

SAMPLE_WAV = {
    "en": os.path.join(ROOT_DIR, "../dnn/whisper.cpp/samples/sample_en.wav"),
    "ru": os.path.join(ROOT_DIR, "../dnn/whisper.cpp/samples/sample_ru.wav")
}
WHISPER = {
    "exec": ["whisper", "-p", "1", "-t", str(CONF["n_thread"]), "-ng", "-oj", "-l"],
    "model": {
        "path": os.path.join(ROOT_DIR, "../dnn/whisper.cpp/models"),
        1: {"lev0": 0.15, "lev1": 0.19, "lev2": 0.92, "lev3": 2.1, "lev4": 3.9},
        2: {"lev0": 0.18, "lev1": 0.25, "lev2": 0.59, "lev3": 1.4, "lev4": 2.5},
    }
}
