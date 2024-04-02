Command to fetch ip address:

```
docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' 83fc1deadb5a
```