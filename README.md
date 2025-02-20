### REST API (flask)

- [ ] **run server**
```bash
flask --app api run --debug --host=0.0.0.0 --port=${PORT}
```

- [ ] **GET**
```bash
curl -X GET ${HOST}:${PORT}/
curl -X GET ${HOST}:${PORT}/${UUID}
```

- [ ] **POST**
```bash
(echo -n '{"lang": "ru", "model": "lev2", "data": "'; base64 inp.mp3; echo '"}') | \
curl -X POST -H "Content-Type: application/json" -d @- ${HOST}:${PORT}/
```
***
### WHISPER.CPP API

- [ ] **build**
```bash
cmake -B Build -DBUILD_SHARED_LIBS=OFF -DGGML_BLAS=ON -DWHISPER_FFMPEG=ON -DWHISPER_OPENVINO=ON \
      -DWHISPER_SDL2=OFF -DCMAKE_CXX_FLAGS="-Wall -std=c++17 -I/usr/include" \
      -DCMAKE_MODULE_LINKER_FLAGS="-Wl -L/usr/lib/x86_64-linux-gnu -licui18n -licuuc -licudata"

cmake --build Build -j --config Release
```

- [ ] **run server**
```bash
whisper-server -t 2 -bo 5 -bs 5 -d 10000 -pr -sns -ss -m ggml.bin -V vocab.txt -H 127.0.0.1 -F

curl 127.0.0.1:8080/rt -H "Content-Type: multipart/form-data" -F file=@v1.ogg
```
***
