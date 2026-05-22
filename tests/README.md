# test env

Isolated docker-compose stack for end-to-end testing without touching
the prod Aiven Postgres or the real ytdl-sub-api.

Stack:
- **pg** — Postgres 16 on tmpfs (state resets every `up`)
- **stub** — minimal Quart app mimicking ytdl-sub-api (`/presets`,
  `/channels` GET/POST/DELETE, `/videos`). In-memory registry.
- **wytchr** — built from the repo root `Dockerfile`, exposed on
  host port **5050** so it doesn't collide with a prod wytchr on `:5000`

Login token is `test-token` (set as `API_TOKEN` for both wytchr and
the stub).

## Usage

```sh
cd tests
docker compose -f compose.test.yml up -d --build

# log in (cookie jar)
curl -c cj.txt -X POST -d "token=test-token" http://localhost:5050/login

# add a channel from a video URL
curl -b cj.txt -X POST \
  --data-urlencode "url=https://www.youtube.com/watch?v=jNQXAC9IVRw" \
  http://localhost:5050/channels/add

# inspect upstream registry
docker exec wytchr-test-wytchr-1 python3 -c \
  "import httpx; print(httpx.get('http://stub:5000/channels', \
   headers={'Authorization':'Bearer test-token'}).text)"

docker compose -f compose.test.yml down -v
```

yt-dlp inside the wytchr container reaches YouTube directly — the host
needs egress.
