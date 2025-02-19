## build

```bash
cmake -B Build -DBUILD_SHARED_LIBS=OFF -DGGML_BLAS=ON -DWHISPER_FFMPEG=ON -DWHISPER_OPENVINO=ON -DWHISPER_SDL2=ON \
      -DCMAKE_CXX_FLAGS="-Wall -std=c++17 -I/usr/include" \
      -DCMAKE_MODULE_LINKER_FLAGS="-Wl -L/usr/lib/x86_64-linux-gnu -licui18n -licuuc -licudata"

cmake --build Build -j --config Release

rm -f ./Build/bin/whisper-server && cmake --build Build -j --config Release -t whisper-server
```

## run

```bash
./Build/bin/whisper-server -t 2 -l ru -bo 5 -bs 5 -d 10000 -pr -sns -ss -m models/ggml-small.bin --convert --vocab vocab.txt

curl 127.0.0.1:8080/rt -H "Content-Type: multipart/form-data" -F file=@v1.ogg
```
