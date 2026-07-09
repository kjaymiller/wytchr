# Developing wytchr locally

The production deploy is the container in `homelab/compose/wytchr/` against Aiven
Postgres. For local iteration you don't want to touch either — so this repo ships
a [`pitchfork.toml`](./pitchfork.toml) that runs the whole stack on your machine:
a throwaway Postgres in Docker plus the Quart app run natively via `uv`, with
hot-restart on every source edit.

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

`.env` is gitignored. `DATABASE_URL` is **not** set there — the dev DB URL lives
in `pitchfork.toml` and points at the local Postgres, so the two never fight.

> Secrets never go in `pitchfork.toml` (it's committed). They stay in `.env`,
> which the app's `run` line sources at start. If you'd rather use pitchfork's
> own uncommitted override, put them in `pitchfork.local.toml` instead — it's
> gitignored and takes precedence.

## Daily loop

```bash
pitchfork start --group wytchr      # postgres first, then web once PG is ready
pitchfork logs -f web               # tail the app (Ctrl-C just detaches)
```

Open <http://127.0.0.1:5050>. Edit any `*.py` or `templates/*.html` and the
`web` daemon restarts itself (the `watch` globs). Postgres keeps running across
those restarts, so your data survives.

```bash
pitchfork stop --group wytchr       # tear the whole stack down
```

## The daemons

| Daemon     | What it is                        | Port / check                     |
|------------|-----------------------------------|----------------------------------|
| `postgres` | `postgres:18-alpine` in Docker    | host `5433`, `pg_isready` gate   |
| `web`      | `hypercorn app:app` via `uv run`  | `127.0.0.1:5050`, TCP ready-port |

- **`depends`** — `web` waits for `postgres` to pass its readiness check before
  starting, so you never hit a "connection refused" on boot.
- **Schema** — the app's `init_db()` runs `CREATE TABLE IF NOT EXISTS` for every
  table, so a fresh volume needs no migration step.
- **Deps** — `uv run --with ...` mirrors the pinned versions in the `Dockerfile`.
  There's no lockfile yet; uv caches wheels so restarts stay fast.

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
- **Port 5433 or 5050 in use** — something else is bound. Change the ports in
  `pitchfork.toml` (`-p 5433:5432` and the `--bind` / `ready_port`), keeping the
  `DATABASE_URL` port in sync.
- **Stale `wytchr-dev-pg` container after a crash** — `docker rm -f wytchr-dev-pg`,
  then start again.
- **`pitchfork` not found** — `mise use -g aqua:jdx/pitchfork`, or ensure mise
  shims are on `PATH` (`eval "$(mise activate zsh)"`).

## What this is *not*

Not a production path. Prod stays on Aiven PG via the homelab container. This
stack is disposable — blow the volume away whenever you like.
