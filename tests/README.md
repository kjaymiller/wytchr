# tests

Two things live here: a **pytest suite** that drives the app in-process,
and a **docker-compose stack** for poking at a real running container by
hand.

## pytest suite

Functional tests against the real Quart app and a real Postgres. Not
unit tests — they go through the HTTP layer and assert on rendered HTML
and committed rows, because every bug they cover lived in the seam
between a SQL query and the template that rendered it.

They need a throwaway Postgres. Easiest is the pitchfork dev instance:

```sh
pitchfork start postgres     # host :5433, the default DATABASE_URL here
uv run --python 3.13 \
  --with 'quart==0.20.*' --with 'apscheduler==3.*' --with 'httpx==0.28.*' \
  --with 'psycopg[binary]==3.*' --with pytest --with pytest-asyncio \
  pytest -q
```

Point `DATABASE_URL` elsewhere to use a different one. **The suite skips
rather than fails when no Postgres is reachable** — check for `skipped`
in the output before trusting a green run.

Tables are truncated between tests, so the DB you point at is not one
you want anything in.

### What's covered, and why

Each test pins a bug that actually shipped or nearly did. They were all
mutation-checked: the fix was reverted and the test confirmed to fail.

| File | Guards against |
| --- | --- |
| `test_bulk_actions.py` | watch-all / skip-all acting on videos the board never showed (shorts, title include/exclude); a skip stamping `watched_at` and so becoming indistinguishable from a watch — and getting swept by `auto_mark_watched` an hour later |
| `test_board.py` | the `actionable` count coming from a SQL `COUNT` blind to the title regexes, so a channel showing one video offers to skip two; empty channels and empty profile sections rendering as scroll distance; the removed `totals` block coming back |
| `test_channels_page.py` | the roster page under-reporting — it inherited the board's totals and has to count every channel, hidden and empty ones included |

CI runs them on every push and PR (`.github/workflows/test.yml`),
against service containers rather than the compose stack below.

## docker-compose stack

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
