# test env

Isolated docker-compose stack for end-to-end testing without touching
the prod Aiven Postgres.

Stack:
- **pg** — Postgres 16 on tmpfs (state resets every `up`)
- **wytchr** — built from the repo root `Dockerfile`, exposed on
  host port **5050** so it doesn't collide with a prod wytchr on `:5000`

Login token is `test-token` (`API_TOKEN`).

`YOUTUBE_API_KEY` defaults to a placeholder. Set a real key in the
environment before bringing the stack up if you need outbound API
calls to succeed:

```sh
export YOUTUBE_API_KEY=...your-real-key...
```

## Usage

```sh
cd tests
docker compose -f compose.test.yml up -d --build

# log in (cookie jar)
curl -c cj.txt -X POST -d "token=test-token" http://localhost:5050/login

# add a channel from a video URL (needs a real YOUTUBE_API_KEY to resolve)
curl -b cj.txt -X POST \
  --data-urlencode "url=https://www.youtube.com/watch?v=jNQXAC9IVRw" \
  http://localhost:5050/channels/add

docker compose -f compose.test.yml down -v
```
