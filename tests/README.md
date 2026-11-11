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
uv sync                      # app + the `dev` group (pytest, playwright)
uv run playwright install chromium   # once, for test_visual.py
uv run pytest -q             # or: mise run test / mise run test:visual
```

### Test credentials

None of these are secret, and none reach YouTube:

| Variable | Test value | What it does |
|---|---|---|
| `API_TOKEN` | `test-token` | wytchr's login. Send `Authorization: Bearer test-token`, or the `wytchr_token=test-token` cookie the login form sets |
| `YOUTUBE_API_KEY` | `AIza` + 35 zeros | passes the boot-time format check; YouTube rejects it |

Nothing in the suite calls the YouTube Data API. Pages that would show
fetched data get it from the database instead: `conftest.py` helpers per
test, and `scripts/seed_fixtures.py` for the visual tests and the factory's
screenshots. Adding a channel by URL (`/channels/add`) and polling do need a
real key, and have no automated coverage.

### Visual tests (Playwright)

`test_visual.py` seeds the DB, starts hypercorn on a free port and opens
every page in Chromium at 320, 390, 820 and 1440px. Each page must return
200 signed in, log no console errors or failed requests, have no broken
images and not scroll sideways. A full-page PNG of each lands in
`.preview/playwright/` (the CI run uploads them as an artifact). It also
checks the signed-out redirect and the token login.

Without a browser these tests skip; `WYTCHR_VISUAL_REQUIRED=1` (set in CI)
makes that a failure. The Python `playwright` is pinned to the version in
the software factory's workcell image, so in a cell it uses the Chromium
that is already installed. Bump both together.

### Software factory

[kjaymiller/factory](https://github.com/kjaymiller/factory) builds PRs for
this repo in a workcell with a throwaway Postgres (and Valkey, on the docker
backend) and no Docker socket. It uses:

- `mise run test` and `mise run lint` as its gates. Both need only `uv`.
- `factory/visual.sh` for the screenshots in each PR: `setup` seeds the
  scratch DB, `serve` runs the app on 127.0.0.1:8000, and `auth` prints
  `AUTH=wytchr_token=test-token`.

htmx and Sortable are vendored in `static/vendor/` so pages render in a
sandbox with no CDN access.

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
| `test_card_render.py` | the single-card htmx swaps (favorite, unhide, fetch-description) 500ing because `_render_card` rendered `_card.html` without the `video_tags_map` the template reads — invisible to every board test, since the board renderers build that map themselves |
| `test_cache.py` | the board read cache going stale (a write that doesn't invalidate leaves watched cards on the board until the TTL expires) and the documented failure policy silently flipping — a Valkey outage must degrade to Postgres, not 500 |
| `test_visual.py` | what only a browser sees: a board whose htmx never loaded (blank, but still a 200), a page that scrolls sideways on a 320px phone, broken thumbnails, the login redirect |

CI runs them on every push and PR (`.github/workflows/test.yml`),
against service containers rather than the compose stack below.

Both Postgres **and** Valkey are truncated/flushed between tests. The
cache reset matters more than it looks: without it TRUNCATE resets
Postgres while Valkey keeps serving the previous test's rendered board,
and since tests reuse channel names the suite starts reading its own
tail. Two tests failed exactly that way before the fixture grew a
`_reset_cache()`.

## docker-compose stack

Isolated docker-compose stack for end-to-end testing without touching
the prod Aiven Postgres.

Stack:
- **pg** — Postgres 16 on tmpfs (state resets every `up`)
- **valkey** — board read cache, no persistence. The app requires
  `VALKEY_URL` and won't boot without it, so this is not optional.
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
