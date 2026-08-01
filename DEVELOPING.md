# Developing wytchr locally

The production deploy is the container in `homelab/compose/wytchr/` against Aiven
Postgres. For local iteration you don't want to touch either — so this repo ships
a [`pitchfork.toml`](./pitchfork.toml) that runs the whole stack on your machine:
a throwaway Postgres and a throwaway Valkey in Docker, plus the Quart app run
natively via `uv`, with hot-restart on every source edit.

[pitchfork](https://pitchfork.jdx.dev) is jdx's process supervisor (same author
as mise, which this repo already uses).

## One-time setup

```bash
# 1. Install pitchfork (via mise — already on this box)
mise use -g aqua:jdx/pitchfork      # or: mise install aqua:jdx/pitchfork

# 2. Create your local env file and fill in the YouTube key
cp .env.example .env
$EDITOR .env                        # set YOUTUBE_API_KEY=...  (API_TOKEN too)
```

`.env` is gitignored. `DATABASE_URL` and `VALKEY_URL` are **not** set there — the
dev URLs live in `pitchfork.toml` and point at the local Postgres and Valkey, so
the two never fight. Both are required by the app (it exits at startup without
them), but `pitchfork start --group wytchr` supplies both, so there is no extra
setup step.

> Secrets never go in `pitchfork.toml` (it's committed). They stay in `.env`,
> which the app's `run` line sources at start. If you'd rather use pitchfork's
> own uncommitted override, put them in `pitchfork.local.toml` instead — it's
> gitignored and takes precedence.

## Daily loop

```bash
pitchfork start --group wytchr      # postgres + valkey first, then web once both are ready
pitchfork logs -f web               # tail the app (Ctrl-C just detaches)
```

Open <http://127.0.0.1:5050>. Edit any `*.py` or `templates/*.html` and the
`web` daemon restarts itself (the `watch` globs). Postgres keeps running across
those restarts, so your data survives.

```bash
pitchfork stop --group wytchr       # tear the whole stack down
```

## The daemons

| Daemon     | What it is                        | Port / check                      |
|------------|-----------------------------------|-----------------------------------|
| `postgres` | `postgres:18-alpine` in Docker    | host `5433`, `pg_isready` gate    |
| `valkey`   | `valkey/valkey:8-alpine` in Docker| host `6380`, `valkey-cli ping` gate |
| `web`      | `hypercorn app:app` via `uv run`  | `127.0.0.1:5050`, TCP ready-port  |

- **`depends`** — `web` waits for *both* `postgres` and `valkey` to pass their
  readiness checks before starting, so you never hit a "connection refused" on
  boot. Valkey is mandatory, not optional: the app backs the board's per-channel
  queries with it and exits at startup if `VALKEY_URL` is unset.
- **Cache data is disposable** — no volume, no persistence. Every key is derived
  from Postgres and re-warms on the next board render, so restarting or deleting
  the container costs nothing but a few queries.
- **Schema** — the app's `init_db()` runs `CREATE TABLE IF NOT EXISTS` for every
  table, so a fresh volume needs no migration step.
- **Deps** — `uv run --with ...` mirrors the pinned versions in the `Dockerfile`.
  There's no lockfile yet; uv caches wheels so restarts stay fast.

## Task runner

`mise.toml` pins the toolchain (Python 3.13, uv, pitchfork) and wraps the
commands you actually type, so a fresh clone doesn't have to memorise them:

```bash
mise install        # get the pinned toolchain
mise run dev        # = pitchfork start --group wytchr
mise run logs       # = pitchfork logs -f web
mise run stop       # = pitchfork stop --group wytchr
mise run test       # functional suite (needs `mise run dev` first)
mise run preview    # render every template with fixture data on :8899, no DB
mise run shots      # screenshot every template at 4 widths + flag overflow
```

Dev and test deps live in `requirements-dev.txt` (the runtime pins there mirror
the `Dockerfile` — keep the two in sync when bumping).

### Looking at layout without a database

`scripts/preview.py` renders every template with stand-in context, so you can
eyeball the chrome without Postgres, Valkey or a YouTube key. `mise run shots`
then loads each page at 320 / 390 / 820 / 1440px, writes PNGs to `.preview/shots/`
(gitignored), and **fails if anything is wider than the viewport** — that
sideways-scroll is the specific thing that makes the site unusable on a phone.

Screenshots need the Playwright browser plus its system libs, once:

```bash
uv run --with-requirements requirements-dev.txt playwright install chromium
# Arch — Playwright's own `install-deps` is Debian-only:
sudo pacman -S --needed libxcomposite libxdamage libxrandr atk at-spi2-atk at-spi2-core
```

## Common tasks

```bash
pitchfork status                    # what's running, PIDs, ports, ready state
pitchfork restart web               # force a restart (e.g. after a dep change)
pitchfork logs postgres             # DB logs
pitchfork run web                   # run in the foreground, attached (Ctrl-C stops)
```

**Reset the database** (drop all local data):

```bash
pitchfork stop postgres
docker volume rm wytchr-dev-pg
pitchfork start --group wytchr      # recreates the volume + schema
```

**Auto start/stop on `cd`** — optional. Install the shell hook once:

```bash
echo 'eval "$(pitchfork activate zsh)"' >> ~/.zshrc   # or: bash
```

then uncomment `auto = ["start", "stop"]` on the `web` daemon in
`pitchfork.toml`. After that, `cd` into this repo brings the stack up and leaving
it tears it down. Left off by default so a stray `cd` doesn't spin up Docker.

## Troubleshooting

- **`web` won't start / auth errors** — `YOUTUBE_API_KEY` missing from `.env`.
  The app fails fast without it. `pitchfork logs web` shows the reason.
- **Port 5433, 6380 or 5050 in use** — something else is bound. Change the ports
  in `pitchfork.toml` (`-p 5433:5432`, `-p 6380:6379`, and the `--bind` /
  `ready_port`), keeping the `DATABASE_URL` / `VALKEY_URL` ports in sync.
- **Stale `wytchr-dev-pg` / `wytchr-dev-valkey` container after a crash** —
  `docker rm -f wytchr-dev-pg wytchr-dev-valkey`, then start again.
- **Board showing stale columns** — an invalidation was missed somewhere. Every
  entry has a TTL so it self-heals, but `docker exec wytchr-dev-valkey valkey-cli
  flushall` clears it immediately.
- **`pitchfork` not found** — `mise use -g aqua:jdx/pitchfork`, or ensure mise
  shims are on `PATH` (`eval "$(mise activate zsh)"`).

## What this is *not*

Not a production path. Prod stays on Aiven PG via the homelab container. This
stack is disposable — blow the volume away whenever you like.
