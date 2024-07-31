import datetime
import json
import os

AUID_LEN = 10
DAYS_TO_SCAN = 8
DAYS_TO_TEST = 8
MAX_CPU = 2
N_THREAD = "2"
ROOT_DIR = os.path.join(os.environ["HOME"], "devel/asr")
SAMPLE_WAV = {
    "en": os.path.join(ROOT_DIR, "../whisper.cpp/samples/sample_en.wav"),
    "ru": os.path.join(ROOT_DIR, "../whisper.cpp/samples/sample_ru.wav")
}
TODAY = datetime.datetime.now()

WHISPER = {
    "exec": "whisper",
    "model": {
        "path": os.path.join(ROOT_DIR, "../whisper.cpp/models"),
        "1": {"lev0": 0.15, "lev1": 0.19, "lev2": 0.92, "lev3": 2.1, "lev4": 3.9},
        "2": {"lev0": 0.18, "lev1": 0.25, "lev2": 0.59, "lev3": 1.4, "lev4": 2.5},
    }
}
with open(os.path.join(ROOT_DIR, "conf.json")) as ff:
    DB_CONF = json.loads(ff.read())
