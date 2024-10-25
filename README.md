### REST API (flask)
***

- [ ] run server
```commandline
flask --app api run --debug --host=0.0.0.0 --port=${PORT}
```

- [ ] method **GET /auid**
```commandline
curl -X GET ${HOST}:${PORT}/${UUID}
```

- [ ] method **POST /**
```commandline
(echo -n '{"lang": "ru", "model": "lev2", "data": "'; base64 inp.mp3; echo '"}') | curl -X POST -H "Content-Type: application/json" -d @- ${HOST}:${PORT}/
```
***