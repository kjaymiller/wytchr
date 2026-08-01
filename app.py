"""wytchr — channel browser + watchlist UI on top of the YouTube Data API.

Polls each channel via YouTube Data API v3 (channels.list, playlistItems.list,
videos.list), surfaces recent uploads in a per-channel column board, and
maintains a per-video watchlist. PostgreSQL is the only state.

Quart + asyncio throughout; all HTTP via httpx.AsyncClient.

Mid-pivot: the channels.preset column and profile UI carry over from the
ytdl-sub-api era and get repurposed in step 6 to mean "auto-watched
window."
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from functools import wraps
from urllib.parse import urlencode

import httpx
import psycopg
import valkey.asyncio as valkey
from psycopg.rows import dict_row
from apscheduler.schedulers.asyncio import AsyncIOScheduler

import youtube_client
from quart import (
    Quart,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
)

__version__ = "0.19.0"

API_TOKEN = os.environ.get("API_TOKEN", "")
# YouTube Data API v3 key. Required — drives channel resolution, the
# poll loop, and description enrichment.
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
# Valkey connection URI for the per-channel board read cache. Required —
# see the "Board read cache" section. `valkey://host:port/db` (or the
# `redis://` alias valkey-py also accepts); `valkeys://`/`rediss://` for
# TLS. compose.yml defaults it to the sibling valkey service.
VALKEY_URL = os.environ.get("VALKEY_URL", "").strip()
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))
POLL_LIMIT = int(os.environ.get("POLL_LIMIT", "30"))

if not API_TOKEN:
    print("FATAL: API_TOKEN env var must be set", file=sys.stderr)
    sys.exit(1)

if not DATABASE_URL:
    print("FATAL: DATABASE_URL env var must be set (Postgres URI from install.sh)", file=sys.stderr)
    sys.exit(1)

if not VALKEY_URL:
    print("FATAL: VALKEY_URL env var must be set (Valkey URI for the board read cache)", file=sys.stderr)
    sys.exit(1)

if not YOUTUBE_API_KEY:
    print(
        "FATAL: YOUTUBE_API_KEY env var must be set (YouTube Data API v3 key)",
        file=sys.stderr,
    )
    sys.exit(1)

# Google API keys are `AIza` + 35 url-safe chars. A common misconfig is
# pasting an OAuth2 access token (`ya29.`/`AQ.`) instead — YouTube then
# rejects every call with 401 "API keys are not supported by this API".
# Warn loudly at boot rather than letting it surface on first use. Not
# fatal, to avoid locking out a valid key in some future format.
if not re.fullmatch(r"AIza[0-9A-Za-z_-]{35}", YOUTUBE_API_KEY):
    print(
        "WARNING: YOUTUBE_API_KEY does not look like a Google API key "
        "(expected 'AIza' + 35 chars). If it starts with 'ya29.' or 'AQ.' "
        "it's an OAuth2 access token, not an API key — channel resolution "
        "and polling will fail with 401. Create an API key under "
        "Google Cloud Console > APIs & Services > Credentials.",
        file=sys.stderr,
    )

app = Quart(__name__)

# Process-wide singletons. All three are created in @app.before_serving
# so they bind to the running event loop, and torn down in
# @app.after_serving. Anywhere outside a request that needs HTTP, the
# cache, or the scheduler should reach for these.
_http_client: httpx.AsyncClient | None = None
_scheduler: AsyncIOScheduler | None = None
_valkey: valkey.Valkey | None = None


@app.context_processor
def inject_version():
    return {"app_version": __version__}


# --- DB ---------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
  name TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  preset TEXT NOT NULL,
  last_polled_at BIGINT,
  last_error TEXT
);
CREATE TABLE IF NOT EXISTS videos (
  video_id TEXT PRIMARY KEY,
  channel_name TEXT NOT NULL,
  title TEXT,
  duration INTEGER,
  upload_date TEXT,
  thumbnail_url TEXT,
  url TEXT,
  status TEXT NOT NULL DEFAULT 'new',
  seen_at BIGINT NOT NULL,
  status_changed_at BIGINT NOT NULL,
  favorited_at BIGINT,
  description TEXT,
  -- Pivot step 3: watchlist/watched timestamps (BIGINT epoch seconds).
  -- NULL on both = "not on watchlist, not watched".
  watchlist_added_at BIGINT,
  watched_at BIGINT
);
CREATE INDEX IF NOT EXISTS videos_channel_status ON videos(channel_name, status);
CREATE INDEX IF NOT EXISTS videos_seen ON videos(seen_at DESC);
CREATE INDEX IF NOT EXISTS videos_favorited ON videos(favorited_at DESC);

-- Outbound webhooks. Fired on `video.favorited` (toggle on); designed
-- to feed downstream services like all-my-favs.
CREATE TABLE IF NOT EXISTS webhooks (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  event TEXT NOT NULL DEFAULT 'video.favorited',
  enabled INTEGER NOT NULL DEFAULT 1,
  bearer_token TEXT,
  created_at BIGINT NOT NULL,
  last_fired_at BIGINT,
  last_status INTEGER,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS webhooks_event_enabled ON webhooks(event, enabled);

-- Tag names are normalized lowercase before insert (see _normalize_tag),
-- so a plain UNIQUE on `name` is effectively case-insensitive.
CREATE TABLE IF NOT EXISTS tags (
  id BIGSERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS video_tags (
  video_id TEXT NOT NULL REFERENCES videos(video_id) ON DELETE CASCADE,
  tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  PRIMARY KEY (video_id, tag_id)
);
CREATE TABLE IF NOT EXISTS channel_tags (
  channel_name TEXT NOT NULL REFERENCES channels(name) ON DELETE CASCADE,
  tag_id BIGINT NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
  PRIMARY KEY (channel_name, tag_id)
);
CREATE INDEX IF NOT EXISTS video_tags_tag ON video_tags(tag_id);
CREATE INDEX IF NOT EXISTS channel_tags_tag ON channel_tags(tag_id);

-- Per-channel overrides. Every column NULLs to "inherit" — the profile
-- (via preset chain) provides the default. UI surfaces these as a form
-- on /channels/<name>/settings.
CREATE TABLE IF NOT EXISTS channel_settings (
  channel_name TEXT PRIMARY KEY REFERENCES channels(name) ON DELETE CASCADE,
  display_name TEXT,
  include_shorts INTEGER NOT NULL DEFAULT 0,
  hide_channel INTEGER NOT NULL DEFAULT 0,
  auto_watched_days INTEGER,
  title_include TEXT,
  title_exclude TEXT,
  updated_at BIGINT NOT NULL
);

-- App-wide key/value preferences (e.g. always_add_channel_to_favs).
CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


# --- DB compatibility shim -------------------------------------------
#
# The codebase was written against sqlite3's surface (`?` placeholders,
# `db.execute(...).fetchone()`, `INSERT OR IGNORE`, `COLLATE NOCASE`).
# Rather than rewrite ~150 call sites to native psycopg-async patterns,
# we wrap psycopg.AsyncConnection in a thin awaitable shim. Call sites
# add `await` in front of `db.execute(...)` (no fetch) or
# `db.execute(...).fetchone()` / `.fetchall()` — that's the entire
# delta from the pre-Quart shape.

_PG_LIKE_SENTINEL = "\x00WYTCHR_PARAM\x00"
_NOCASE_RE = re.compile(r"\s+COLLATE\s+NOCASE", re.IGNORECASE)


def _to_pg_sql(sql: str, has_params: bool) -> str:
    # ORDER BY ... COLLATE NOCASE → ORDER BY LOWER(...) so case-insensitive
    # ordering survives the move to PG (which lacks NOCASE).
    sql = re.sub(
        r"ORDER BY\s+([\w\.]+)\s+COLLATE\s+NOCASE",
        r"ORDER BY LOWER(\1)",
        sql,
        flags=re.IGNORECASE,
    )
    sql = _NOCASE_RE.sub("", sql)
    # `INSERT OR IGNORE INTO ...` → `INSERT INTO ... ON CONFLICT DO NOTHING`.
    # Append on the trailing end so it works for both VALUES and SELECT forms.
    or_ignore = re.search(r"\bINSERT\s+OR\s+IGNORE\b", sql, re.IGNORECASE) is not None
    if or_ignore:
        sql = re.sub(r"\bINSERT\s+OR\s+IGNORE\b", "INSERT", sql, flags=re.IGNORECASE)
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    # SQLite-style `?` → psycopg `%s`. When params are present, also
    # escape any literal `%` (e.g. inside LIKE '%/shorts/%') so psycopg's
    # pyformat parser doesn't mistake it for a placeholder.
    if has_params:
        sql = sql.replace("?", _PG_LIKE_SENTINEL)
        sql = sql.replace("%", "%%")
        sql = sql.replace(_PG_LIKE_SENTINEL, "%s")
    else:
        sql = sql.replace("?", "%s")
    return sql


class _RowResult:
    """Awaitable wrapper that lets the call-site shapes
    `await db.execute(sql)`, `await db.execute(sql).fetchone()`, and
    `await db.execute(sql).fetchall()` all work without intermediate
    variables. SQL execution is deferred until first await so the
    chained `.fetchone()` form doesn't double-execute.
    """

    __slots__ = ("_conn", "_sql", "_params", "_cur")

    def __init__(self, conn: psycopg.AsyncConnection, sql: str, params):
        self._conn = conn
        self._sql = sql
        self._params = params
        self._cur: psycopg.AsyncCursor | None = None

    async def _exec(self):
        if self._cur is None:
            cur = self._conn.cursor()
            if self._params is None:
                await cur.execute(self._sql)
            else:
                await cur.execute(self._sql, self._params)
            self._cur = cur
        return self._cur

    def __await__(self):
        return self._exec().__await__()

    async def fetchone(self):
        cur = await self._exec()
        return await cur.fetchone()

    async def fetchall(self):
        cur = await self._exec()
        return await cur.fetchall()


class AsyncPgConn:
    """sqlite3.Connection-shaped (async) wrapper around psycopg."""

    def __init__(self, raw: psycopg.AsyncConnection):
        self._conn = raw

    @classmethod
    async def connect(cls, dsn: str) -> "AsyncPgConn":
        raw = await psycopg.AsyncConnection.connect(
            dsn, row_factory=dict_row, autocommit=False
        )
        return cls(raw)

    def execute(self, sql: str, params=None) -> _RowResult:
        pg_sql = _to_pg_sql(sql, params is not None and params != ())
        return _RowResult(self._conn, pg_sql, params)

    async def executescript(self, script: str) -> None:
        # Split on `;` and run statements individually — psycopg3's
        # execute() doesn't reliably handle multi-statement strings.
        # Strips comment lines so `--` comments don't accidentally
        # consume a trailing statement.
        cleaned = "\n".join(
            line for line in script.splitlines() if not line.strip().startswith("--")
        )
        async with self._conn.cursor() as cur:
            for stmt in cleaned.split(";"):
                if stmt.strip():
                    await cur.execute(_to_pg_sql(stmt, has_params=False))

    async def commit(self) -> None:
        await self._conn.commit()

    async def rollback(self) -> None:
        await self._conn.rollback()

    async def close(self) -> None:
        await self._conn.close()


async def get_db() -> AsyncPgConn:
    db = getattr(g, "_db", None)
    if db is None:
        db = await AsyncPgConn.connect(DATABASE_URL)
        g._db = db
    return db


@app.teardown_appcontext
async def close_db(_exc):
    db = getattr(g, "_db", None)
    if db is not None:
        await db.close()


@asynccontextmanager
async def standalone_db():
    """For background jobs running outside a request context."""
    db = await AsyncPgConn.connect(DATABASE_URL)
    try:
        yield db
        await db.commit()
    finally:
        await db.close()


async def init_db() -> None:
    async with standalone_db() as db:
        await db.executescript(SCHEMA)
        # Idempotent migrations for installs that predate columns added
        # later. PG supports ADD COLUMN IF NOT EXISTS natively (no need
        # for the introspection dance SQLite required).
        await db.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS favorited_at BIGINT")
        await db.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS description TEXT")
        await db.execute("CREATE INDEX IF NOT EXISTS videos_favorited ON videos(favorited_at DESC)")
        # Pivot step 3: watchlist + watched-state columns. Additive only
        # — existing rows are NULL on both columns, which reads as
        # "not on watchlist, not watched".
        await db.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS watchlist_added_at BIGINT")
        await db.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS watched_at BIGINT")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS videos_watchlist ON videos(watchlist_added_at DESC) "
            "WHERE watchlist_added_at IS NOT NULL"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS videos_channel_unwatched "
            "ON videos(channel_name) WHERE watched_at IS NULL"
        )
        # Pivot step 7: drop ytdl-sub/Jellyfin-era columns. Forward
        # migration; PG's IF EXISTS makes it a no-op on fresh installs.
        # Also collapse download-era status values to 'new' so the
        # status enum becomes (new, watched, hidden).
        await db.execute("ALTER TABLE videos DROP COLUMN IF EXISTS last_output")
        await db.execute("ALTER TABLE channel_settings DROP COLUMN IF EXISTS date_range")
        await db.execute("ALTER TABLE channel_settings DROP COLUMN IF EXISTS max_files")
        await db.execute("ALTER TABLE channel_settings DROP COLUMN IF EXISTS include_members_only")
        await db.execute(
            "UPDATE videos SET status = 'new' "
            "WHERE status IN ('queued', 'downloading', 'failed', 'done')"
        )


# --- Tags -------------------------------------------------------------

_CHANNEL_SETTINGS_DEFAULTS = {
    "display_name": None,
    "include_shorts": 0,
    "hide_channel": 0,
    "auto_watched_days": None,
    "title_include": None,
    "title_exclude": None,
}


async def _get_setting(db: AsyncPgConn, key: str, default: str = "") -> str:
    row = await db.execute(
        "SELECT value FROM app_settings WHERE key = ?", (key,)
    ).fetchone()
    return row["value"] if row else default


async def _set_setting(db: AsyncPgConn, key: str, value: str) -> None:
    await db.execute(
        "INSERT INTO app_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value),
    )


async def _channel_settings_map(db: AsyncPgConn) -> dict[str, dict]:
    """Fetch every channel's settings row into {name: dict}. Channels
    without a row return defaults via .get(name, _CHANNEL_SETTINGS_DEFAULTS)
    at the call site."""
    rows = await db.execute(
        """SELECT channel_name, display_name,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude
             FROM channel_settings"""
    ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        out[r["channel_name"]] = {
            "display_name": r["display_name"],
            "include_shorts": int(r["include_shorts"] or 0),
            "hide_channel": int(r["hide_channel"] or 0),
            "auto_watched_days": r["auto_watched_days"],
            "title_include": r["title_include"],
            "title_exclude": r["title_exclude"],
        }
    return out


def _compile_title_re(pattern: str | None):
    """Channels store free-form regex strings. Bad patterns become
    no-ops rather than crash the whole board render."""
    if not pattern:
        return None
    try:
        return re.compile(pattern, re.IGNORECASE)
    except re.error:
        return None


_TAG_RE = re.compile(r"[^a-z0-9_-]+")


def _normalize_tag(name: str) -> str:
    """Lowercase, slug-ify, cap at 64 chars. Empty result = invalid.

    A single colon partitions a tag into `prefix:suffix` (the grouping
    convention). Each side is slugified independently, then rejoined.
    Multiple colons are collapsed to one (`a:b:c` becomes `a:bc`),
    keeping group structure flat.
    """
    name = (name or "").strip().lower()
    if not name:
        return ""
    if ":" in name:
        prefix, _, suffix = name.partition(":")
        prefix = _TAG_RE.sub("-", prefix).strip("-")
        suffix = _TAG_RE.sub("-", suffix).strip("-")
        if not suffix:
            return prefix[:64]
        if not prefix:
            return suffix[:64]
        return f"{prefix}:{suffix}"[:64]
    return _TAG_RE.sub("-", name).strip("-")[:64]


def _split_tag_prefix(name: str) -> tuple[str, str]:
    """('sport:mlb',) → ('sport', 'mlb'); ('mlb',) → ('', 'mlb')."""
    if ":" in name:
        prefix, _, suffix = name.partition(":")
        return prefix, suffix
    return "", name


async def _upsert_tag(db: AsyncPgConn, name: str) -> int | None:
    norm = _normalize_tag(name)
    if not norm:
        return None
    await db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (norm,))
    row = await db.execute("SELECT id FROM tags WHERE name = ?", (norm,)).fetchone()
    return row["id"] if row else None


async def _video_tags_map(db: AsyncPgConn, video_ids: list[str]) -> dict[str, list[str]]:
    if not video_ids:
        return {}
    placeholders = ",".join("?" * len(video_ids))
    rows = await db.execute(
        f"""SELECT vt.video_id, t.name
              FROM video_tags vt
              JOIN tags t ON t.id = vt.tag_id
             WHERE vt.video_id IN ({placeholders})
          ORDER BY t.name COLLATE NOCASE""",
        video_ids,
    ).fetchall()
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["video_id"], []).append(r["name"])
    return out


async def _channel_tags_map(db: AsyncPgConn, names: list[str]) -> dict[str, list[str]]:
    if not names:
        return {}
    placeholders = ",".join("?" * len(names))
    rows = await db.execute(
        f"""SELECT ct.channel_name, t.name
              FROM channel_tags ct
              JOIN tags t ON t.id = ct.tag_id
             WHERE ct.channel_name IN ({placeholders})
          ORDER BY t.name COLLATE NOCASE""",
        names,
    ).fetchall()
    out: dict[str, list[str]] = {}
    for r in rows:
        out.setdefault(r["channel_name"], []).append(r["name"])
    return out


# --- Board read cache (Valkey) ----------------------------------------
#
# Every board render runs two queries *per channel* — a `GROUP BY status`
# count and a windowed video select — and the board auto-refreshes every
# 30s per client on top of a full re-render after every bulk action. With
# N channels that's 2N round-trips to Aiven PG on a 30-second cadence for
# data that only moves when a poll lands or the operator clicks something.
# Valkey sits in front of the per-channel slice (see _channel_slice) and
# absorbs the repeats.
#
# What's cached: the structured rows (`videos` / `counts` / `actionable`),
# not the rendered HTML. The column's *markup* depends on more than its
# query does — show_hidden, the active tag (every chip href embeds it),
# and the per-video/per-channel tag chips that come from board-wide
# batched lookups — so caching HTML would either multiply the key space
# by every template-affecting flag or serve wrong markup. Rows keep the
# key space sane and leave the template free to change; only a change to
# the *selected columns* needs the schema version below bumped.
#
# Failure policy: boot-required, runtime-forgiving. A missing VALKEY_URL
# is fatal at import (above), but a Valkey error mid-request degrades to
# Postgres rather than 500ing — see _cache_degrade.

# Key: wytchr:slice:<schema>:<channel>:<view_hash>. Bump the schema
# segment when the cached payload's *shape* changes (new selected column,
# renamed field); every old entry is then simply never read again and
# ages out on its own TTL, so a rollout needs no manual flush.
CACHE_SCHEMA = "v1"
CACHE_PREFIX = f"wytchr:slice:{CACHE_SCHEMA}"

# Socket timeouts are deliberately tight. The cache exists to *save*
# time; a Valkey that takes longer to answer than Postgres would is worse
# than no cache, so we give up fast and read through.
CACHE_SOCKET_TIMEOUT = 1.0

_cache_last_warn = 0.0


def _cache_degrade(op: str, exc: Exception) -> None:
    """Single choke point for every runtime cache failure.

    Everything in the cache is derived from Postgres and there is no
    write-back, so a Valkey error is indistinguishable from a cache miss
    to the caller — reading through is always correct, just slower.
    Failing the request instead would turn a cache blip into a total
    outage, and would fail the *write* routes (which call
    _invalidate_channel after the commit already landed) for no
    correctness gain.

    To flip to fully-required semantics, `raise exc` here — no call site
    changes.

    Warnings are rate-limited to one per minute so a sustained outage
    doesn't drown the log at the board's 30s refresh cadence.
    """
    global _cache_last_warn
    now = time.monotonic()
    if now - _cache_last_warn < 60:
        return
    _cache_last_warn = now
    print(
        f"WARNING: board cache unavailable ({op}: {exc}) — reading through to "
        "Postgres. Further cache warnings suppressed for 60s.",
        file=sys.stderr,
        flush=True,
    )


def _cache() -> valkey.Valkey | None:
    """Return the process-wide Valkey client. None before before_serving
    has run (scripts, imports) — callers treat that as a miss."""
    return _valkey


# TTL bounds. The floor keeps a badly-clocked last_polled_at from
# producing a 1-second TTL that makes the cache pure overhead; the
# ceiling caps how stale a very quiet channel can get if every explicit
# invalidation somehow missed.
CACHE_TTL_MIN = 15
CACHE_TTL_MAX = 3600
CACHE_TTL_POLL_DUE = 30


def _slice_ttl(last_polled_at: int | None, cs: dict, now: int | None = None) -> int:
    """Per-channel TTL, derived from that channel's own rules.

    A global constant would be wrong in both directions: a channel polled
    every 30 minutes doesn't need a 60-second TTL, and a channel whose
    poll is overdue shouldn't hold a stale slice for half an hour. The
    formula:

        ttl = (last_polled_at + POLL_INTERVAL_MINUTES * 60) - now
            → seconds until the next poll is expected to land, which is
              the next moment this channel's videos can change on their
              own. This is the upper bound the issue asks for.

        ttl <= 0            → CACHE_TTL_POLL_DUE (30s). Never polled, or
                              the poll is due/overdue: a write is
                              imminent, so stay short.

        hide_channel        → max(ttl, CACHE_TTL_MAX). A hidden channel
                              isn't on the default board at all; only an
                              explicit unhide changes what it shows, and
                              that path invalidates.

        auto_watched_days   → min(ttl, 3600). auto_mark_watched() runs
                              hourly and only touches channels with a
                              window set. It invalidates what it marks,
                              but capping at one sweep period means a
                              missed invalidation self-heals within one
                              sweep instead of riding to the ceiling.

        clamped to [CACHE_TTL_MIN, CACHE_TTL_MAX].

    The shorts/title filters are the *other* channel rules the issue
    names; they don't move the TTL because they're folded into the key
    instead (see _slice_key) — editing them changes the hash, so the old
    entry is never read again rather than needing to expire.
    """
    if now is None:
        now = int(time.time())
    ttl = ((last_polled_at or 0) + POLL_INTERVAL_MINUTES * 60) - now
    if ttl <= 0:
        ttl = CACHE_TTL_POLL_DUE
    if cs.get("hide_channel"):
        ttl = max(ttl, CACHE_TTL_MAX)
    if cs.get("auto_watched_days"):
        ttl = min(ttl, 3600)
    return max(CACHE_TTL_MIN, min(ttl, CACHE_TTL_MAX))


def _slice_key(
    channel_name: str,
    cs: dict,
    *,
    show_hidden: bool,
    tag_filter_name: str,
    limit: int,
) -> str:
    """Cache key for one channel's board slice.

    The hash has to discriminate on everything that changes the query
    result: `show_hidden` (picks the status filter), the tag filter, the
    row limit, and the channel's own settings — include_shorts and
    title_include/title_exclude change which rows survive, hide_channel
    changes whether the column renders at all, display_name rides along
    in the payload. Putting the settings in the *key* rather than on an
    invalidation hook means a settings edit can't serve rows filtered by
    the previous regex: the new hash simply misses.

    Channel names are slugified to [A-Za-z0-9_-] on insert, so they
    contain no `:` (the delimiter) and no glob metacharacters — which is
    what makes the SCAN pattern in _invalidate_channel safe.
    """
    view = json.dumps(
        [
            bool(show_hidden),
            tag_filter_name or "",
            int(limit),
            cs.get("display_name") or "",
            int(cs.get("include_shorts") or 0),
            int(cs.get("hide_channel") or 0),
            cs.get("title_include") or "",
            cs.get("title_exclude") or "",
        ],
        separators=(",", ":"),
    )
    digest = hashlib.sha256(view.encode("utf-8")).hexdigest()[:16]
    return f"{CACHE_PREFIX}:{channel_name}:{digest}"


async def _cache_get(key: str) -> dict | None:
    client = _cache()
    if client is None:
        return None
    try:
        raw = await client.get(key)
    except Exception as exc:  # noqa: BLE001 — any cache error reads through
        _cache_degrade("get", exc)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        # Corrupt or older-shaped entry. Treat as a miss; it'll be
        # overwritten by the set that follows.
        return None


async def _cache_set(key: str, payload: dict, ttl: int) -> None:
    client = _cache()
    if client is None:
        return
    try:
        await client.set(key, json.dumps(payload), ex=ttl)
    except Exception as exc:  # noqa: BLE001
        _cache_degrade("set", exc)


async def _invalidate_channel(channel_name: str | None) -> None:
    """Drop every cached slice for one channel.

    TTL is the safety net; this is the correctness mechanism — it runs on
    every write path that changes what a channel's column shows.

    SCAN rather than a computed key list because the tag filter makes the
    view dimension open-ended: a write path can't enumerate which views
    exist. The alternative (a per-channel generation counter folded into
    the key) would cost an extra round-trip on the *read* path, which is
    the hot one — the board refreshes every 30s per client while writes
    are operator clicks.
    """
    client = _cache()
    if client is None or not channel_name:
        return
    try:
        keys = [k async for k in client.scan_iter(match=f"{CACHE_PREFIX}:{channel_name}:*", count=200)]
        if keys:
            await client.unlink(*keys)
    except Exception as exc:  # noqa: BLE001 — a failed invalidation degrades
        # to TTL-bounded staleness, which is the whole point of having a TTL.
        _cache_degrade("invalidate", exc)


async def _invalidate_all() -> None:
    """Drop every cached slice. For the tag admin operations (rename with
    merge, delete) that repoint video_tags across arbitrarily many
    channels at once — cheaper to reason about than working out the
    affected set."""
    client = _cache()
    if client is None:
        return
    try:
        keys = [k async for k in client.scan_iter(match=f"{CACHE_PREFIX}:*", count=500)]
        if keys:
            await client.unlink(*keys)
    except Exception as exc:  # noqa: BLE001
        _cache_degrade("invalidate-all", exc)


async def _channel_of_video(db: AsyncPgConn, video_id: str) -> str | None:
    """The per-video routes only know a video id, but cache keys are
    per-channel — so the invalidating routes need this one extra lookup.
    channel_name never changes for a given video, so it's equally valid
    before or after the UPDATE."""
    row = await db.execute(
        "SELECT channel_name FROM videos WHERE video_id = ?", (video_id,)
    ).fetchone()
    return row["channel_name"] if row else None


async def _invalidate_video(db: AsyncPgConn, video_id: str) -> None:
    """_invalidate_channel for a route that only has a video id."""
    await _invalidate_channel(await _channel_of_video(db, video_id))


# --- Auth -------------------------------------------------------------

def _authed() -> bool:
    cookie = request.cookies.get("wytchr_token")
    if cookie and cookie == API_TOKEN:
        return True
    header = request.headers.get("Authorization", "")
    return header == f"Bearer {API_TOKEN}"


def auth_required(fn):
    @wraps(fn)
    async def wrapper(*a, **kw):
        if not _authed():
            if request.headers.get("HX-Request"):
                return ("unauthorized", 401)
            if request.method == "GET" and request.accept_mimetypes.accept_html:
                return redirect("/login")
            return jsonify({"error": "unauthorized"}), 401
        return await fn(*a, **kw)
    return wrapper


# --- HTTP client ------------------------------------------------------

def _client() -> httpx.AsyncClient:
    """Return the process-wide AsyncClient. before_serving must have run."""
    if _http_client is None:
        # Defensive: ad-hoc fallback if someone calls this outside the
        # serving lifecycle (tests, scripts). Caller leaks the client;
        # acceptable for the one-off path.
        return httpx.AsyncClient()
    return _http_client


# --- Polling ----------------------------------------------------------

_CHANNEL_URL_ID_RE = re.compile(r"youtube\.com/channel/(UC[\w-]{22})", re.I)


async def _resolve_channel_id_for_poll(db: AsyncPgConn, name: str, url: str) -> str:
    """Return the UC... channel ID for a stored channel row.

    Fast path: extract from /channel/UC... URLs (the canonical shape
    new entries are stored in). Slow path: resolve via YouTube API
    once and update channels.url to the canonical form so subsequent
    polls take the fast path.
    """
    m = _CHANNEL_URL_ID_RE.search(url or "")
    if m:
        return m.group(1)
    resolved = await youtube_client.resolve_channel(
        _client(), url, api_key=YOUTUBE_API_KEY
    )
    canonical = resolved["url"]
    await db.execute(
        "UPDATE channels SET url = ? WHERE name = ?", (canonical, name)
    )
    return resolved["channel_id"]


_YOUTUBE_FEED_URL = "https://www.youtube.com/feeds/videos.xml?channel_id={}"


def _feed_url_from_channel_url(url: str) -> str | None:
    """Native YouTube RSS feed URL for a channel, derived from its stored
    URL. Returns None when the URL isn't in canonical /channel/UC... form
    yet (a just-added channel gets canonicalized on its first poll)."""
    m = _CHANNEL_URL_ID_RE.search(url or "")
    return _YOUTUBE_FEED_URL.format(m.group(1)) if m else None


def _isoduration_seconds(iso: str | None) -> int | None:
    """Parse ISO 8601 duration (PT1H2M3S) to seconds. Returns None if
    missing/unparseable. Only handles the H/M/S subset YouTube emits."""
    if not iso:
        return None
    m = re.fullmatch(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", iso)
    if not m:
        return None
    h, mi, s = (int(g) if g else 0 for g in m.groups())
    return h * 3600 + mi * 60 + s


def _ymd_from_iso(iso: str | None) -> str | None:
    """publishedAt (`2026-05-22T14:00:00Z`) → upload_date `YYYYMMDD`."""
    if not iso or len(iso) < 10:
        return None
    return iso[:4] + iso[5:7] + iso[8:10]


async def _resolve_channel(url: str) -> dict:
    """Resolve a video or channel URL to its channel.

    Accepts the URL shapes supported by youtube_client.resolve_channel:
    /watch?v=, youtu.be/, /channel/UC..., /@handle. Returns
    {channel_url, channel_name, handle}. Raises RuntimeError on failure
    so the caller's existing except-and-redirect path keeps working.
    """
    if not YOUTUBE_API_KEY:
        raise RuntimeError("YOUTUBE_API_KEY is not configured")
    try:
        resolved = await youtube_client.resolve_channel(
            _client(), url, api_key=YOUTUBE_API_KEY
        )
    except (youtube_client.YouTubeAPIError, ValueError) as exc:
        raise RuntimeError(str(exc)) from exc
    handle = resolved.get("handle") or ""
    title = resolved.get("title") or handle
    return {
        "channel_url": resolved["url"],
        "channel_name": title,
        "handle": handle,
    }


async def poll_channel(db: AsyncPgConn, name: str, url: str) -> tuple[int, str | None]:
    """Returns (new_video_count, error_message).

    Hits the YouTube Data API for channel uploads + per-video details
    (duration). No subprocess, no ytdl-sub-api involvement.
    """
    now = int(time.time())
    if not YOUTUBE_API_KEY:
        msg = "YOUTUBE_API_KEY is not configured"
        await db.execute(
            "UPDATE channels SET last_polled_at = ?, last_error = ? WHERE name = ?",
            (now, msg, name),
        )
        return 0, msg
    try:
        channel_id = await _resolve_channel_id_for_poll(db, name, url)
        uploads = await youtube_client.list_channel_uploads(
            _client(), channel_id, api_key=YOUTUBE_API_KEY, limit=POLL_LIMIT
        )
    except Exception as exc:  # noqa: BLE001
        await db.execute(
            "UPDATE channels SET last_polled_at = ?, last_error = ? WHERE name = ?",
            (now, str(exc)[:500], name),
        )
        return 0, str(exc)

    if not uploads:
        await db.execute(
            "UPDATE channels SET last_polled_at = ?, last_error = NULL WHERE name = ?",
            (now, name),
        )
        return 0, None

    # Pull duration (+ richer snippet) in a single batch call. ISO 8601
    # `PT1H2M3S` shapes are parsed to seconds. Up to 50 IDs per call.
    video_ids = [u["video_id"] for u in uploads if u.get("video_id")]
    try:
        details = await youtube_client.get_videos(
            _client(), video_ids, api_key=YOUTUBE_API_KEY
        )
    except Exception:  # noqa: BLE001
        details = []
    detail_by_id = {d["id"]: d for d in details if d.get("id")}

    row = await db.execute(
        "SELECT include_shorts FROM channel_settings WHERE channel_name = ?",
        (name,),
    ).fetchone()
    include_shorts = bool(row and row["include_shorts"])

    new_count = 0
    for u in uploads:
        vid = u.get("video_id")
        if not vid:
            continue
        detail = detail_by_id.get(vid) or {}
        duration_iso = ((detail.get("contentDetails") or {}).get("duration"))
        duration = _isoduration_seconds(duration_iso)
        # Proxy for shorts: <=60s. YouTube API doesn't surface the
        # /shorts/ URL the way yt-dlp's flat-playlist did. Imperfect
        # (some non-short videos are also <=60s), but matches the
        # operator's intent of skipping bite-sized content by default.
        if duration is not None and duration <= 60 and not include_shorts:
            continue
        title = u.get("title")
        upload_date = _ymd_from_iso(u.get("published_at"))
        thumb = u.get("thumbnail_url") or f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"
        watch_url = f"https://www.youtube.com/watch?v={vid}"
        existing = await db.execute(
            "SELECT status FROM videos WHERE video_id = ?", (vid,)
        ).fetchone()
        if existing is None:
            await db.execute(
                """INSERT INTO videos
                   (video_id, channel_name, title, duration, upload_date,
                    thumbnail_url, url, status, seen_at, status_changed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)""",
                (vid, name, title, duration, upload_date, thumb, watch_url, now, now),
            )
            new_count += 1
        else:
            await db.execute(
                """UPDATE videos
                      SET title = COALESCE(?, title),
                          duration = COALESCE(?, duration),
                          upload_date = COALESCE(?, upload_date),
                          thumbnail_url = COALESCE(?, thumbnail_url),
                          url = COALESCE(?, url)
                    WHERE video_id = ?""",
                (title, duration, upload_date, thumb, watch_url, vid),
            )
    await db.execute(
        "UPDATE channels SET last_polled_at = ?, last_error = NULL WHERE name = ?",
        (now, name),
    )
    return new_count, None


# Asyncio-native poll coordinator. `running` is the in-flight flag —
# both the scheduler and the manual /poll/all route consult it so a
# user clicking "refresh" mid-cycle doesn't kick a second concurrent
# poll. `summary` carries the most recent finished run so /poll/status
# can hand it back to the board, which uses it to fire the existing
# `wytchr:polled` toast.
_poll_state: dict = {
    "running": False,
    "started_at": None,
    "summary": None,
    "summary_id": 0,
    "task": None,
}
_poll_lock = asyncio.Lock()


async def _poll_all_impl() -> dict:
    """Poll each channel. Caller owns the run-flag."""
    started = time.time()
    summary: dict = {"channels": 0, "new_videos": 0, "errors": []}
    try:
        async with standalone_db() as db:
            rows = await db.execute("SELECT name, url FROM channels").fetchall()
            await db.commit()
            summary["channels"] = len(rows)
            for row in rows:
                count, err = await poll_channel(db, row["name"], row["url"])
                # Commit per channel so other writers can interleave
                # instead of waiting for the whole sweep.
                await db.commit()
                # Ingest is the main non-operator write path. Invalidate
                # on every channel the sweep touched, not just the ones
                # that gained videos: poll_channel always writes
                # last_polled_at, and last_polled_at is what the TTL is
                # derived from.
                await _invalidate_channel(row["name"])
                summary["new_videos"] += count
                if err:
                    summary["errors"].append(f"{row['name']}: {err}")
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"fatal: {exc}")
    summary["elapsed_seconds"] = round(time.time() - started, 2)
    print(f"poll_all: {summary}", file=sys.stderr, flush=True)
    return summary


async def poll_all() -> dict:
    """Scheduler entry point. Honors the in-flight guard so a manual
    refresh doesn't race the next tick."""
    async with _poll_lock:
        if _poll_state["running"]:
            return {"channels": 0, "new_videos": 0, "errors": [],
                    "elapsed_seconds": 0, "skipped": "already_running"}
        _poll_state["running"] = True
        _poll_state["started_at"] = time.time()
    summary: dict | None = None
    try:
        summary = await _poll_all_impl()
        return summary
    finally:
        async with _poll_lock:
            if summary is not None:
                _poll_state["summary"] = summary
                _poll_state["summary_id"] += 1
            _poll_state["running"] = False
            _poll_state["started_at"] = None


async def _start_poll_async() -> bool:
    """Kick poll_all as a fire-and-forget task. Returns True if this
    call actually launched the worker, False if one was already running.
    The /poll/all route returns immediately so the request worker is
    freed; the board polls /poll/status to discover completion.
    """
    async with _poll_lock:
        if _poll_state["running"]:
            return False
        _poll_state["running"] = True
        _poll_state["started_at"] = time.time()

    async def _runner():
        try:
            summary = await _poll_all_impl()
        except Exception as exc:  # noqa: BLE001 — defensive; task must not vanish silently
            summary = {"channels": 0, "new_videos": 0,
                       "errors": [f"fatal: {exc}"], "elapsed_seconds": 0}
        async with _poll_lock:
            _poll_state["summary"] = summary
            _poll_state["summary_id"] += 1
            _poll_state["running"] = False
            _poll_state["started_at"] = None
            _poll_state["task"] = None

    _poll_state["task"] = asyncio.create_task(_runner(), name="wytchr-poll")
    return True


# --- Routes -----------------------------------------------------------

@app.get("/healthz")
async def healthz():
    return jsonify({"ok": True})


@app.get("/login")
async def login_form():
    return await render_template("login.html", error=None)


@app.post("/login")
async def login_submit():
    form = await request.form
    token = (form.get("token") or "").strip()
    if token != API_TOKEN:
        return await render_template("login.html", error="invalid token"), 401
    resp = redirect("/")
    resp.set_cookie(
        "wytchr_token", token, httponly=True, samesite="Lax", max_age=60 * 60 * 24 * 30
    )
    return resp


@app.get("/")
@auth_required
async def index():
    # Profiles come from the distinct set of channels.preset values,
    # which the UI uses as free-form section labels.
    db = await get_db()
    rows = await db.execute(
        "SELECT preset FROM channels WHERE preset IS NOT NULL AND preset <> '' "
        "GROUP BY preset ORDER BY preset COLLATE NOCASE"
    ).fetchall()
    profiles = [r["preset"] for r in rows]
    return await render_template(
        "board.html",
        poll_interval=POLL_INTERVAL_MINUTES,
        profiles=profiles,
    )


# Rows per channel column. Module-level because _channel_slice keys on
# it — a change here has to change the cache key too, or the first render
# after the bump serves the old row count.
PER_CHANNEL_LIMIT = 20


async def _channel_slice(
    db: AsyncPgConn,
    ch: dict,
    cs: dict,
    *,
    show_hidden: bool,
    tag_filter_id: int | None,
    tag_filter_name: str,
    limit: int = PER_CHANNEL_LIMIT,
    use_cache: bool = True,
) -> dict:
    """One channel's board slice: {videos, counts, actionable,
    channel_tag_match}.

    The unit of caching, and the unit of work the board and the manage-
    channels page share. `use_cache=False` skips both the read and the
    write and goes straight to Postgres — the manage page is the
    operator's "what does the DB actually say" view, so a stale
    management screen would defeat its purpose.

    `channel_tag_match` is only meaningful under a tag filter: it says
    the *channel* carries the tag, which means the column stays on the
    board even when it has no matching videos.
    """
    key = ""
    if use_cache:
        key = _slice_key(
            ch["name"],
            cs,
            show_hidden=show_hidden,
            tag_filter_name=tag_filter_name,
            limit=limit,
        )
        cached = await _cache_get(key)
        if cached is not None:
            return cached

    if show_hidden:
        status_filter = ("hidden",)
    else:
        # wytchr is a selector, not a library view: only show videos
        # the operator hasn't acted on yet.
        status_filter = ("new",)
    status_placeholders = ",".join("?" * len(status_filter))

    shorts_clause = "" if cs["include_shorts"] else " AND v.url NOT LIKE '%/shorts/%'"
    shorts_clause_count = "" if cs["include_shorts"] else " AND url NOT LIKE '%/shorts/%'"
    title_inc_re = _compile_title_re(cs["title_include"])
    title_exc_re = _compile_title_re(cs["title_exclude"])

    count_rows = await db.execute(
        f"SELECT status, COUNT(*) AS n FROM videos WHERE channel_name = ? {shorts_clause_count} GROUP BY status",
        (ch["name"],),
    ).fetchall()
    counts = {row["status"]: row["n"] for row in count_rows}
    actionable = counts.get("new", 0)
    if title_inc_re or title_exc_re:
        # SQL can't see the title regexes, so the raw 'new' count would
        # promise more than the column shows — and the bulk-action
        # confirm copy reads off this number. Recount the hard way.
        actionable = len(await _actionable_video_ids(db, ch["name"], cs))

    window_clause = ""
    window_args: tuple = ()

    channel_tag_match = False
    if tag_filter_id is not None:
        channel_has_tag = await db.execute(
            "SELECT 1 FROM channel_tags WHERE channel_name = ? AND tag_id = ?",
            (ch["name"], tag_filter_id),
        ).fetchone()
        channel_tag_match = bool(channel_has_tag)
        if channel_has_tag:
            videos = await db.execute(
                f"""SELECT v.video_id, v.title, v.duration, v.upload_date,
                           v.thumbnail_url, v.url, v.status, v.favorited_at,
                           v.description
                      FROM videos v
                     WHERE v.channel_name = ?
                       AND v.status IN ({status_placeholders})
                       {shorts_clause}
                       {window_clause}
                  ORDER BY v.upload_date DESC NULLS LAST, v.seen_at DESC
                     LIMIT ?""",
                (ch["name"], *status_filter, *window_args, limit),
            ).fetchall()
        else:
            videos = await db.execute(
                f"""SELECT v.video_id, v.title, v.duration, v.upload_date,
                           v.thumbnail_url, v.url, v.status, v.favorited_at,
                           v.description
                      FROM videos v
                      JOIN video_tags vt ON vt.video_id = v.video_id
                     WHERE v.channel_name = ?
                       AND v.status IN ({status_placeholders})
                       AND vt.tag_id = ?
                       {window_clause}
                  ORDER BY v.upload_date DESC NULLS LAST, v.seen_at DESC
                     LIMIT ?""",
                (ch["name"], *status_filter, tag_filter_id, *window_args, limit),
            ).fetchall()
    else:
        videos = await db.execute(
            f"""SELECT v.video_id, v.title, v.duration, v.upload_date,
                       v.thumbnail_url, v.url, v.status
                  FROM videos v
                 WHERE v.channel_name = ?
                   AND v.status IN ({status_placeholders})
                   {shorts_clause}
                   {window_clause}
              ORDER BY v.upload_date DESC NULLS LAST, v.seen_at DESC
                 LIMIT ?""",
            (ch["name"], *status_filter, *window_args, limit),
        ).fetchall()

    # Title filters run in Python (they're free-form regex, not SQL
    # LIKE), so the cached payload is the *post-filter* list — a hit
    # skips this too.
    if title_inc_re or title_exc_re:
        kept = []
        for v in videos:
            title = v["title"] or ""
            if title_inc_re and not title_inc_re.search(title):
                continue
            if title_exc_re and title_exc_re.search(title):
                continue
            kept.append(v)
        videos = kept

    payload = {
        "videos": videos,
        "counts": counts,
        "actionable": actionable,
        "channel_tag_match": channel_tag_match,
    }
    if use_cache:
        await _cache_set(
            key, payload, _slice_ttl(ch.get("last_polled_at"), cs)
        )
    return payload


@app.get("/board")
@auth_required
async def board_partial():
    db = await get_db()
    channels = await db.execute(
        "SELECT name, url, preset, last_polled_at, last_error FROM channels ORDER BY name COLLATE NOCASE"
    ).fetchall()
    show_hidden = request.args.get("hidden") == "1"
    tag_filter_name = _normalize_tag(request.args.get("tag", ""))
    tag_filter_id: int | None = None
    if tag_filter_name:
        row = await db.execute(
            "SELECT id FROM tags WHERE name = ?", (tag_filter_name,)
        ).fetchone()
        tag_filter_id = row["id"] if row else -1

    settings_map = await _channel_settings_map(db)

    columns = []
    for ch in channels:
        cs = settings_map.get(ch["name"], _CHANNEL_SETTINGS_DEFAULTS)
        if cs["hide_channel"] and not show_hidden:
            continue

        slice_ = await _channel_slice(
            db,
            ch,
            cs,
            show_hidden=show_hidden,
            tag_filter_id=tag_filter_id,
            tag_filter_name=tag_filter_name,
        )
        videos = slice_["videos"]
        counts = slice_["counts"]
        actionable = slice_["actionable"]


        # A channel with nothing left after filtering is just scroll
        # distance — drop the column entirely.
        if not videos:
            continue

        ch_dict = dict(ch)
        ch_dict["display_name"] = cs["display_name"] or ch["name"]
        ch_dict["feed_url"] = _feed_url_from_channel_url(ch["url"])
        default_open = (
            (counts.get("hidden", 0) > 0) if show_hidden else (actionable > 0)
        )
        columns.append(
            {
                "channel": ch_dict,
                "videos": videos,
                "counts": counts,
                "actionable": actionable,
                "default_open": default_open,
            }
        )
    await db.commit()

    sections: list[dict] = []
    section_idx: dict[str, int] = {}
    for col in columns:
        profile_name = (col["channel"].get("preset") or "").strip()
        if profile_name not in section_idx:
            section_idx[profile_name] = len(sections)
            sections.append({"profile": profile_name, "columns": []})
        sections[section_idx[profile_name]]["columns"].append(col)
    sections.sort(key=lambda s: (s["profile"] == "", s["profile"].lower()))
    # Sections are built from non-empty columns only, so a profile with
    # nothing to show never gets created in the first place.

    all_video_ids = [v["video_id"] for col in columns for v in col["videos"]]
    video_tags_map = await _video_tags_map(db, all_video_ids)
    channel_tags_map = await _channel_tags_map(db, [c["channel"]["name"] for c in columns])

    all_tags = await db.execute(
        """SELECT t.name,
                  (SELECT COUNT(*) FROM video_tags WHERE tag_id = t.id) AS video_count,
                  (SELECT COUNT(*) FROM channel_tags WHERE tag_id = t.id) AS channel_count
             FROM tags t
         ORDER BY t.name COLLATE NOCASE"""
    ).fetchall()

    grouped_tags: dict[str, list] = {}
    for t in all_tags:
        prefix, suffix = _split_tag_prefix(t["name"])
        grouped_tags.setdefault(prefix, []).append({
            "name": t["name"],
            "suffix": suffix,
            "video_count": t["video_count"],
            "channel_count": t["channel_count"],
        })

    # No board-wide totals: they were summed from the rendered columns,
    # which no longer include the channels we drop for being empty.
    # Roster-wide counts live on the channels manage page, which counts
    # in Postgres instead of guessing from a filtered view.
    return await render_template(
        "_board.html",
        columns=columns,
        sections=sections,
        show_hidden=show_hidden,
        has_channels=bool(channels),
        video_tags_map=video_tags_map,
        channel_tags_map=channel_tags_map,
        all_tags=all_tags,
        grouped_tags=grouped_tags,
        active_tag=tag_filter_name or None,
    )


async def _name_from_request() -> str:
    if request.is_json:
        body = await request.get_json(silent=True)
        return ((body or {}).get("name") or "").strip()
    form = await request.form
    return (form.get("name") or "").strip()


async def _render_video_tags(db: AsyncPgConn, video_id: str):
    rows = await db.execute(
        """SELECT t.name FROM video_tags vt
             JOIN tags t ON t.id = vt.tag_id
            WHERE vt.video_id = ?
         ORDER BY t.name COLLATE NOCASE""",
        (video_id,),
    ).fetchall()
    tags = [r["name"] for r in rows]
    return await render_template("_tags_video.html", video_id=video_id, tags=tags)


async def _render_channel_tags(db: AsyncPgConn, channel_name: str):
    rows = await db.execute(
        """SELECT t.name FROM channel_tags ct
             JOIN tags t ON t.id = ct.tag_id
            WHERE ct.channel_name = ?
         ORDER BY t.name COLLATE NOCASE""",
        (channel_name,),
    ).fetchall()
    tags = [r["name"] for r in rows]
    return await render_template("_tags_channel.html", channel_name=channel_name, tags=tags)


@app.post("/videos/<video_id>/tags")
@auth_required
async def add_video_tag(video_id: str):
    db = await get_db()
    if not await db.execute("SELECT 1 FROM videos WHERE video_id = ?", (video_id,)).fetchone():
        return ("video not found", 404)
    tag_id = await _upsert_tag(db, await _name_from_request())
    if tag_id is None:
        return await _render_video_tags(db, video_id)
    await db.execute(
        "INSERT OR IGNORE INTO video_tags (video_id, tag_id) VALUES (?, ?)",
        (video_id, tag_id),
    )
    await db.commit()
    # Tag membership decides whether this video appears under a
    # tag-filtered board view.
    await _invalidate_video(db, video_id)
    return await _render_video_tags(db, video_id)


@app.delete("/videos/<video_id>/tags/<tag_name>")
@auth_required
async def delete_video_tag(video_id: str, tag_name: str):
    db = await get_db()
    norm = _normalize_tag(tag_name)
    await db.execute(
        """DELETE FROM video_tags
                 WHERE video_id = ?
                   AND tag_id = (SELECT id FROM tags WHERE name = ?)""",
        (video_id, norm),
    )
    await db.commit()
    await _invalidate_video(db, video_id)
    return await _render_video_tags(db, video_id)


@app.post("/channels/<channel_name>/tags")
@auth_required
async def add_channel_tag(channel_name: str):
    db = await get_db()
    if not await db.execute(
        "SELECT 1 FROM channels WHERE name = ?", (channel_name,)
    ).fetchone():
        return ("channel not found", 404)
    tag_id = await _upsert_tag(db, await _name_from_request())
    if tag_id is None:
        return await _render_channel_tags(db, channel_name)
    await db.execute(
        "INSERT OR IGNORE INTO channel_tags (channel_name, tag_id) VALUES (?, ?)",
        (channel_name, tag_id),
    )
    await db.commit()
    # A channel tag flips channel_tag_match, which decides whether the
    # column survives a tag filter at all.
    await _invalidate_channel(channel_name)
    return await _render_channel_tags(db, channel_name)


@app.delete("/channels/<channel_name>/tags/<tag_name>")
@auth_required
async def delete_channel_tag(channel_name: str, tag_name: str):
    db = await get_db()
    norm = _normalize_tag(tag_name)
    await db.execute(
        """DELETE FROM channel_tags
                 WHERE channel_name = ?
                   AND tag_id = (SELECT id FROM tags WHERE name = ?)""",
        (channel_name, norm),
    )
    await db.commit()
    await _invalidate_channel(channel_name)
    return await _render_channel_tags(db, channel_name)


@app.get("/tags")
@auth_required
async def tags_admin():
    db = await get_db()
    rows = await db.execute(
        """SELECT t.id, t.name,
                  (SELECT COUNT(*) FROM video_tags WHERE tag_id = t.id) AS video_count,
                  (SELECT COUNT(*) FROM channel_tags WHERE tag_id = t.id) AS channel_count
             FROM tags t
         ORDER BY t.name COLLATE NOCASE"""
    ).fetchall()
    grouped: dict[str, list] = {}
    for r in rows:
        prefix, suffix = _split_tag_prefix(r["name"])
        grouped.setdefault(prefix, []).append({
            "id": r["id"],
            "name": r["name"],
            "suffix": suffix,
            "video_count": r["video_count"],
            "channel_count": r["channel_count"],
        })
    flash = request.args.get("flash") or ""
    return await render_template("tags.html", grouped=grouped, flash=flash)


def _humanize_ago(ts: int | None, now: int) -> str:
    """Epoch seconds → 'never' / 'just now' / '5m ago' / '3h ago' / '2d ago'.
    Poll times are only ever read at a glance, so order of magnitude is
    all the operator needs."""
    if not ts:
        return "never"
    delta = max(0, now - int(ts))
    if delta < 60:
        return "just now"
    if delta < 3600:
        return f"{delta // 60}m ago"
    if delta < 86400:
        return f"{delta // 3600}h ago"
    return f"{delta // 86400}d ago"


@app.get("/channels")
@auth_required
async def channels_admin():
    db = await get_db()
    channels = await db.execute(
        "SELECT name, url, preset, last_polled_at, last_error FROM channels ORDER BY name COLLATE NOCASE"
    ).fetchall()
    settings_map = await _channel_settings_map(db)
    # One grouped pass over videos for the whole roster. The board does
    # this per channel, which is tolerable when most columns are skipped
    # but silly on a page whose entire job is to show every channel.
    # Shorts are split out here so the counts can honour each channel's
    # include_shorts without a second query.
    count_rows = await db.execute(
        """SELECT channel_name, status,
                  (url LIKE '%/shorts/%') AS is_short,
                  COUNT(*) AS n
             FROM videos
         GROUP BY channel_name, status, (url LIKE '%/shorts/%')"""
    ).fetchall()
    counts_map: dict[str, dict[str, int]] = {}
    shorts_map: dict[str, dict[str, int]] = {}
    for r in count_rows:
        target = shorts_map if r["is_short"] else counts_map
        bucket = target.setdefault(r["channel_name"], {})
        bucket[r["status"]] = bucket.get(r["status"], 0) + r["n"]

    now = int(time.time())
    rows = []
    for ch in channels:
        cs = settings_map.get(ch["name"], _CHANNEL_SETTINGS_DEFAULTS)
        counts = dict(counts_map.get(ch["name"], {}))
        if cs["include_shorts"]:
            for status, n in shorts_map.get(ch["name"], {}).items():
                counts[status] = counts.get(status, 0) + n
        display_name = cs["display_name"] or ch["name"]
        rows.append(
            {
                "name": ch["name"],
                "url": ch["url"],
                "preset": (ch["preset"] or "").strip(),
                "display_name": display_name,
                # Only worth showing the real name when it isn't the label.
                "aliased": display_name != ch["name"],
                "include_shorts": cs["include_shorts"],
                "hide_channel": cs["hide_channel"],
                "auto_watched_days": cs["auto_watched_days"],
                "new": counts.get("new", 0),
                "watched": counts.get("watched", 0),
                "hidden": counts.get("hidden", 0),
                "last_polled_at": ch["last_polled_at"] or 0,
                "last_polled": _humanize_ago(ch["last_polled_at"], now),
                "last_error": ch["last_error"],
            }
        )
    # Roster-wide summary. The board used to carry these four figures in
    # its own header; it can't any more now that its columns are served
    # from a cache, so this page — which always reads Postgres — owns
    # them. Every channel counts, hidden and empty ones included.
    totals = {
        "channels": len(rows),
        "videos": sum(r["new"] + r["watched"] + r["hidden"] for r in rows),
        "new": sum(r["new"] for r in rows),
        "watched": sum(r["watched"] for r in rows),
        "hidden_channels": sum(1 for r in rows if r["hide_channel"]),
        "erroring": sum(1 for r in rows if r["last_error"]),
    }
    return await render_template("channels.html", channels=rows, totals=totals)


@app.get("/channels/add")
@auth_required
async def channel_add_page():
    db = await get_db()
    rows = await db.execute(
        "SELECT preset FROM channels WHERE preset IS NOT NULL AND preset <> '' "
        "GROUP BY preset ORDER BY preset COLLATE NOCASE"
    ).fetchall()
    profiles = [r["preset"] for r in rows]
    always_favs = await _get_setting(db, "always_add_channel_to_favs") == "1"
    return await render_template(
        "channel_add.html",
        profiles=profiles,
        error=request.args.get("error"),
        prefill_url=request.args.get("url", ""),
        always_favs=always_favs,
    )


_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")


def _channel_id_from_url(url: str) -> str:
    """Last path segment without an `@` prefix, slugified. Fallback
    channel name when the resolver couldn't extract a handle."""
    tail = (url or "").rstrip("/").rsplit("/", 1)[-1]
    return _NAME_RE.sub("-", tail.lstrip("@")).strip("-")


def _add_channel_error(msg: str, **extra: str):
    """Redirect back to the add form with a URL-encoded error. Encoding
    matters: resolve failures surface raw API error bodies (with newlines,
    quotes, &) that would otherwise produce an invalid Location header."""
    return redirect("/channels/add?" + urlencode({"error": msg, **extra}))


@app.post("/channels/add")
@auth_required
async def channel_add():
    f = await request.form
    raw_url = (f.get("url") or "").strip()
    override_name = (f.get("name") or "").strip()
    profile = (f.get("profile") or "").strip()
    add_to_favs = bool(f.get("add_to_favs"))
    if not raw_url:
        return _add_channel_error("url is required")

    try:
        resolved = await _resolve_channel(raw_url)
    except Exception as exc:  # noqa: BLE001
        return _add_channel_error(f"could not resolve: {str(exc)[:200]}", url=raw_url)

    channel_url = resolved["channel_url"]
    name = override_name or resolved["handle"] or _channel_id_from_url(channel_url)
    name = _NAME_RE.sub("-", name).strip("-")
    if not name:
        return _add_channel_error("could not derive a name", url=raw_url)

    # Write to the local channels table. The `profile` column gets
    # repurposed in pivot step 6 (auto-mark-watched window); for now it
    # round-trips the form value as a free-form label.
    async with standalone_db() as db:
        existing = await db.execute(
            "SELECT 1 FROM channels WHERE name = ?", (name,)
        ).fetchone()
        if existing:
            return _add_channel_error("already subscribed", url=raw_url)
        await db.execute(
            "INSERT INTO channels (name, url, preset) VALUES (?, ?, ?)",
            (name, channel_url, profile),
        )
        if not add_to_favs:
            add_to_favs = await _get_setting(db, "always_add_channel_to_favs") == "1"
        await db.commit()
    if add_to_favs:
        # Fire-and-forget so the redirect isn't blocked on downstream.
        asyncio.create_task(
            _fire_webhooks("channel.added", _channel_payload(name, channel_url))
        )
    await _start_poll_async()
    return redirect("/")


@app.post("/channels/<channel_name>/profile")
@auth_required
async def channel_change_profile(channel_name: str):
    """Reassign a channel's profile label (local DB only)."""
    body = await request.get_json(silent=True) or {}
    new_profile = (body.get("profile") or "").strip()
    db = await get_db()
    if not await db.execute(
        "SELECT 1 FROM channels WHERE name = ?", (channel_name,)
    ).fetchone():
        return jsonify({"error": "channel not found"}), 404
    await db.execute(
        "UPDATE channels SET preset = ? WHERE name = ?",
        (new_profile, channel_name),
    )
    await db.commit()
    return jsonify({"ok": True, "preset": new_profile})


# --- Per-channel settings ---------------------------------------------

async def _get_channel_settings_row(db: AsyncPgConn, channel_name: str) -> dict:
    r = await db.execute(
        """SELECT display_name,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude
             FROM channel_settings WHERE channel_name = ?""",
        (channel_name,),
    ).fetchone()
    if r is None:
        return dict(_CHANNEL_SETTINGS_DEFAULTS)
    return {
        "display_name": r["display_name"],
        "include_shorts": int(r["include_shorts"] or 0),
        "hide_channel": int(r["hide_channel"] or 0),
        "auto_watched_days": r["auto_watched_days"],
        "title_include": r["title_include"],
        "title_exclude": r["title_exclude"],
    }


@app.get("/channels/<channel_name>/settings")
@auth_required
async def channel_settings_page(channel_name: str):
    db = await get_db()
    ch = await db.execute(
        "SELECT name, url, preset FROM channels WHERE name = ?", (channel_name,)
    ).fetchone()
    if not ch:
        return ("channel not found", 404)
    settings = await _get_channel_settings_row(db, channel_name)
    return await render_template(
        "channel_settings.html",
        channel=ch,
        settings=settings,
        feed_url=_feed_url_from_channel_url(ch["url"]),
        saved=request.args.get("saved") == "1",
    )


@app.post("/channels/<channel_name>/settings")
@auth_required
async def channel_settings_save(channel_name: str):
    db = await get_db()
    if not await db.execute("SELECT 1 FROM channels WHERE name = ?", (channel_name,)).fetchone():
        return ("channel not found", 404)
    f = await request.form

    def _opt_str(key: str) -> str | None:
        v = (f.get(key) or "").strip()
        return v or None

    def _opt_int(key: str) -> int | None:
        v = (f.get(key) or "").strip()
        if not v:
            return None
        try:
            n = int(v)
            return n if n > 0 else None
        except ValueError:
            return None

    payload = {
        "display_name": _opt_str("display_name"),
        "include_shorts": 1 if f.get("include_shorts") else 0,
        "hide_channel": 1 if f.get("hide_channel") else 0,
        "auto_watched_days": _opt_int("auto_watched_days"),
        "title_include": _opt_str("title_include"),
        "title_exclude": _opt_str("title_exclude"),
    }
    for key in ("title_include", "title_exclude"):
        if payload[key] and _compile_title_re(payload[key]) is None:
            return (f"invalid regex for {key}: {payload[key]}", 400)

    await db.execute(
        """INSERT INTO channel_settings
             (channel_name, display_name,
              include_shorts, hide_channel, auto_watched_days,
              title_include, title_exclude, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(channel_name) DO UPDATE SET
             display_name = excluded.display_name,
             include_shorts = excluded.include_shorts,
             hide_channel = excluded.hide_channel,
             auto_watched_days = excluded.auto_watched_days,
             title_include = excluded.title_include,
             title_exclude = excluded.title_exclude,
             updated_at = excluded.updated_at""",
        (
            channel_name,
            payload["display_name"],
            payload["include_shorts"],
            payload["hide_channel"],
            payload["auto_watched_days"],
            payload["title_include"],
            payload["title_exclude"],
            int(time.time()),
        ),
    )
    await db.commit()
    return redirect(f"/channels/{channel_name}/settings?saved=1")


@app.post("/tags/create")
@auth_required
async def create_tag():
    f = await request.form
    raw = (f.get("name") or "").strip()
    norm = _normalize_tag(raw)
    if not norm:
        return redirect("/tags?flash=invalid+name")
    db = await get_db()
    existing = await db.execute("SELECT 1 FROM tags WHERE name = ?", (norm,)).fetchone()
    if existing:
        return redirect(f"/tags?flash=tag+{norm}+already+exists")
    await _upsert_tag(db, norm)
    await db.commit()
    return redirect(f"/tags?flash=created+{norm}")


@app.post("/tags/<old_name>/rename")
@auth_required
async def rename_tag(old_name: str):
    f = await request.form
    new_raw = (f.get("new_name") or "").strip()
    new_norm = _normalize_tag(new_raw)
    if not new_norm:
        return redirect("/tags?flash=invalid+name")
    db = await get_db()
    old_row = await db.execute("SELECT id FROM tags WHERE name = ?", (old_name,)).fetchone()
    if not old_row:
        return redirect("/tags?flash=tag+not+found")
    if old_name == new_norm:
        return redirect("/tags")
    old_id = old_row["id"]
    existing = await db.execute(
        "SELECT id FROM tags WHERE name = ?", (new_norm,)
    ).fetchone()
    if existing and existing["id"] != old_id:
        # Merge: repoint joins from old → existing, drop the old tag.
        keep_id = existing["id"]
        await db.execute(
            "INSERT OR IGNORE INTO video_tags (video_id, tag_id) "
            "SELECT video_id, ? FROM video_tags WHERE tag_id = ?",
            (keep_id, old_id),
        )
        await db.execute("DELETE FROM video_tags WHERE tag_id = ?", (old_id,))
        await db.execute(
            "INSERT OR IGNORE INTO channel_tags (channel_name, tag_id) "
            "SELECT channel_name, ? FROM channel_tags WHERE tag_id = ?",
            (keep_id, old_id),
        )
        await db.execute("DELETE FROM channel_tags WHERE tag_id = ?", (old_id,))
        await db.execute("DELETE FROM tags WHERE id = ?", (old_id,))
        await db.commit()
        # A merge repoints video_tags/channel_tags across arbitrarily
        # many channels at once — cheaper to drop everything than to
        # work out the affected set.
        await _invalidate_all()
        return redirect(f"/tags?flash=merged+into+{new_norm}")
    await db.execute("UPDATE tags SET name = ? WHERE id = ?", (new_norm, old_id))
    await db.commit()
    return redirect(f"/tags?flash=renamed+to+{new_norm}")


@app.post("/tags/<name>/delete")
@auth_required
async def delete_tag(name: str):
    db = await get_db()
    await db.execute("DELETE FROM tags WHERE name = ?", (name,))
    await db.commit()
    # ON DELETE CASCADE takes video_tags/channel_tags rows with it, again
    # across an unknown set of channels.
    await _invalidate_all()
    return redirect(f"/tags?flash=deleted+{name}")


@app.post("/poll/all")
@auth_required
async def poll_all_route():
    """Kick poll_all as a background task; return immediately. The board
    polls /poll/status until the run lands, then fires the existing
    wytchr:polled toast. Frees the worker so the page stays interactive
    during the 30+s sweep.
    """
    started = await _start_poll_async()
    async with _poll_lock:
        state = {
            "running": _poll_state["running"],
            "started": started,
            "summary_id": _poll_state["summary_id"],
        }
    if request.headers.get("HX-Request"):
        resp = await make_response("", 204)
        resp.headers["HX-Trigger"] = json.dumps({"wytchr:polling-started": state})
        return resp
    return jsonify(state)


@app.get("/poll/status")
@auth_required
async def poll_status_route():
    async with _poll_lock:
        payload = {
            "running": _poll_state["running"],
            "started_at": _poll_state["started_at"],
            "summary_id": _poll_state["summary_id"],
            "summary": _poll_state["summary"],
        }
    return jsonify(payload)


@app.post("/videos/<video_id>/hide")
@auth_required
async def hide_video(video_id: str):
    db = await get_db()
    await db.execute(
        "UPDATE videos SET status = 'hidden', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    await db.commit()
    await _invalidate_video(db, video_id)
    if request.headers.get("HX-Request"):
        return ("", 200)
    return jsonify({"ok": True})


@app.post("/videos/<video_id>/watched")
@auth_required
async def watched_video(video_id: str):
    db = await get_db()
    now = int(time.time())
    await db.execute(
        "UPDATE videos SET status = 'watched', status_changed_at = ?, "
        "watched_at = COALESCE(watched_at, ?) WHERE video_id = ?",
        (now, now, video_id),
    )
    await db.commit()
    await _invalidate_video(db, video_id)
    if request.headers.get("HX-Request"):
        return ("", 200)
    return jsonify({"ok": True})


async def _actionable_video_ids(
    db: AsyncPgConn, channel_name: str, cs: dict | None = None
) -> list[str]:
    """The 'new' videos of a channel the board would actually show.

    Bulk actions must not touch videos the operator never saw, so the
    per-channel filters apply here too: shorts as a SQL clause, the
    title include/exclude regexes in Python (same as board_partial).
    Callers that already have the channel's settings row pass it in."""
    if cs is None:
        cs = (await _channel_settings_map(db)).get(channel_name, _CHANNEL_SETTINGS_DEFAULTS)
    shorts_clause = "" if cs["include_shorts"] else " AND url NOT LIKE '%/shorts/%'"
    rows = await db.execute(
        f"SELECT video_id, title FROM videos "
        f"WHERE channel_name = ? AND status = 'new'{shorts_clause}",
        (channel_name,),
    ).fetchall()
    title_inc_re = _compile_title_re(cs["title_include"])
    title_exc_re = _compile_title_re(cs["title_exclude"])
    ids = []
    for r in rows:
        title = r["title"] or ""
        if title_inc_re and not title_inc_re.search(title):
            continue
        if title_exc_re and title_exc_re.search(title):
            continue
        ids.append(r["video_id"])
    return ids


async def _bulk_channel_action(channel_name: str, action: str):
    """Shared body of watch-all / skip-all.

    'watch' archives what the operator engaged with (watched_at set);
    'skip' is the dismissal pile — status_changed_at only, watched_at
    left NULL so the auto_mark_watched sweep never reads a skip as a
    view. Both stay reversible through ?hidden=1 / unhide."""
    db = await get_db()
    if not await db.execute("SELECT 1 FROM channels WHERE name = ?", (channel_name,)).fetchone():
        return ("channel not found", 404)
    video_ids = await _actionable_video_ids(db, channel_name)
    now = int(time.time())
    marked = 0
    if video_ids:
        placeholders = ",".join("?" * len(video_ids))
        if action == "watch":
            cur = await db.execute(
                f"""UPDATE videos
                      SET status = 'watched', status_changed_at = ?,
                          watched_at = COALESCE(watched_at, ?)
                    WHERE video_id IN ({placeholders})""",
                (now, now, *video_ids),
            )
        else:
            cur = await db.execute(
                f"""UPDATE videos
                      SET status = 'hidden', status_changed_at = ?
                    WHERE video_id IN ({placeholders})""",
                (now, *video_ids),
            )
        marked = cur.rowcount or 0
    await db.commit()
    # Must precede the board_partial() re-render below, or the response
    # is rebuilt from the slice we just invalidated the truth out of.
    # Shared by watch-all and skip-all, so both are covered here.
    await _invalidate_channel(channel_name)
    if request.headers.get("HX-Request"):
        body = await board_partial()
        resp = await make_response(body)
        resp.headers["HX-Trigger"] = json.dumps({
            "wytchr:channel-bulk": {
                "channel": channel_name, "action": action, "marked": marked
            }
        })
        return resp
    return jsonify({"ok": True, "action": action, "marked": marked})


@app.post("/channels/<channel_name>/watch-all")
@auth_required
async def watch_channel_all(channel_name: str):
    """Bulk-flip every actionable video in the channel to 'watched'."""
    return await _bulk_channel_action(channel_name, "watch")


@app.post("/channels/<channel_name>/skip-all")
@auth_required
async def skip_channel_all(channel_name: str):
    """Bulk-dismiss every actionable video in the channel to 'hidden'."""
    return await _bulk_channel_action(channel_name, "skip")


@app.post("/videos/<video_id>/unhide")
@auth_required
async def unhide_video(video_id: str):
    db = await get_db()
    await db.execute(
        "UPDATE videos SET status = 'new', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    await db.commit()
    await _invalidate_video(db, video_id)
    if request.headers.get("HX-Request"):
        return await _render_card(video_id)
    return jsonify({"ok": True})


async def _render_card(video_id: str):
    """Re-render one card for an htmx swap.

    `_card.html` reads `video_tags_map` — the bulk renderers build it
    once for the whole board, so this path has to build its own
    one-entry map. Omitting it doesn't degrade to an untagged card;
    Jinja raises UndefinedError and the swap 500s.
    """
    db = await get_db()
    row = await db.execute(
        """SELECT video_id, title, duration, upload_date, thumbnail_url, url, status, channel_name, favorited_at, description
             FROM videos WHERE video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return ("", 404)
    tags_map = await _video_tags_map(db, [video_id])
    return await render_template("_card.html", v=row, video_tags_map=tags_map)


# --- Favorites + webhooks --------------------------------------------

def _video_payload(row) -> dict:
    # Receivers (e.g. all-my-favs) typically expect bookmark fields at
    # the top level: url, title, notes. We map description→notes.
    desc = row.get("description") if isinstance(row, dict) else None
    return {
        "id": row["video_id"],
        "title": row["title"],
        "url": row["url"],
        "channel": row["channel_name"],
        "thumbnail_url": row["thumbnail_url"],
        "duration": row["duration"],
        "upload_date": row["upload_date"],
        "description": desc,
        "notes": desc,
    }


def _channel_payload(name: str, url: str) -> dict:
    # all-my-favs expects bookmark fields at the top level: url, title,
    # notes. A channel has no description, so notes is left empty.
    return {"name": name, "title": name, "url": url, "notes": ""}


_VIDEO_ID_RE = re.compile(r"(?:v=|/shorts/|youtu\.be/)([A-Za-z0-9_-]{11})")


async def _enrich_and_fire(video_id: str) -> None:
    """Bg task: backfill description (if missing), then fire the
    favorite webhook. Caller has already toggled favorited_at on and
    spawned this via asyncio.create_task."""
    async with standalone_db() as db:
        row = await db.execute(
            """SELECT video_id, title, duration, upload_date, thumbnail_url,
                      url, status, channel_name, favorited_at, description
                 FROM videos WHERE video_id = ?""",
            (video_id,),
        ).fetchone()
        if not row:
            return
        if not row["description"] and row["url"]:
            desc = await _fetch_description(row["url"])
            if desc:
                await db.execute(
                    "UPDATE videos SET description = ? WHERE video_id = ?",
                    (desc, video_id),
                )
                await db.commit()
                row = await db.execute(
                    """SELECT video_id, title, duration, upload_date, thumbnail_url,
                              url, status, channel_name, favorited_at, description
                         FROM videos WHERE video_id = ?""",
                    (video_id,),
                ).fetchone()
                await _invalidate_channel(row["channel_name"])
    await _fire_webhooks("video.favorited", _video_payload(row))


async def _fetch_description(video_url: str) -> str | None:
    """Pull a video's description via the YouTube Data API v3."""
    if not YOUTUBE_API_KEY:
        return None
    m = _VIDEO_ID_RE.search(video_url)
    if not m:
        return None
    try:
        r = await _client().get(
            "https://www.googleapis.com/youtube/v3/videos",
            params={"id": m.group(1), "part": "snippet", "key": YOUTUBE_API_KEY},
            timeout=10.0,
        )
        if r.status_code != 200:
            return None
        items = r.json().get("items") or []
        if not items:
            return None
        desc = (items[0].get("snippet") or {}).get("description")
        return desc.strip() if desc else None
    except Exception:
        return None


async def _fire_webhooks(event: str, payload: dict, hook_id: int | None = None) -> None:
    """POST to webhooks. Designed to be spawned via asyncio.create_task
    when fire-and-forget; await directly when the caller wants to know
    the result landed (none of the call sites do, today)."""
    async with standalone_db() as db:
        if hook_id is not None:
            hooks = await db.execute(
                "SELECT id, url, bearer_token FROM webhooks WHERE id = ?",
                (hook_id,),
            ).fetchall()
        else:
            hooks = await db.execute(
                "SELECT id, url, bearer_token FROM webhooks WHERE event = ? AND enabled = 1",
                (event,),
            ).fetchall()
    body = {"event": event, "timestamp": int(time.time()), **payload}
    for h in hooks:
        headers = {"Content-Type": "application/json", "User-Agent": "wytchr/1.0"}
        if h["bearer_token"]:
            headers["Authorization"] = f"Bearer {h['bearer_token']}"
        status: int | None = None
        err: str | None = None
        try:
            r = await _client().post(h["url"], json=body, headers=headers, timeout=10.0)
            status = r.status_code
            if r.status_code >= 400:
                err = (r.text or "")[:500]
        except Exception as e:
            err = str(e)[:500]
        async with standalone_db() as db:
            await db.execute(
                "UPDATE webhooks SET last_fired_at = ?, last_status = ?, last_error = ? WHERE id = ?",
                (int(time.time()), status, err, h["id"]),
            )


@app.post("/videos/<video_id>/favorite")
@auth_required
async def favorite_video(video_id: str):
    db = await get_db()
    row = await db.execute(
        """SELECT v.video_id, v.title, v.duration, v.upload_date, v.thumbnail_url,
                  v.url, v.status, v.channel_name, v.favorited_at, v.description
             FROM videos v WHERE v.video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    now = int(time.time())
    if row["favorited_at"]:
        await db.execute("UPDATE videos SET favorited_at = NULL WHERE video_id = ?", (video_id,))
        await db.commit()
        favorited = False
    else:
        await db.execute("UPDATE videos SET favorited_at = ? WHERE video_id = ?", (now, video_id))
        await db.commit()
        favorited = True
        # Backfill description + fire the webhook as a background task
        # so the UI returns immediately. Description has to land before
        # the POST or the receiver gets notes=null.
        asyncio.create_task(_enrich_and_fire(video_id))
    # favorited_at rides along in the tag-filtered slice payload, so a
    # toggle leaves the star stale on that view without this.
    await _invalidate_channel(row["channel_name"])
    if request.headers.get("HX-Request"):
        body = await _render_card(video_id)
        resp = await make_response(body)
        title = (row["title"] or row["video_id"])[:80]
        webhook_count = 0
        if favorited:
            count_row = await db.execute(
                "SELECT COUNT(*) AS n FROM webhooks WHERE event = 'video.favorited' AND enabled = 1"
            ).fetchone()
            webhook_count = count_row["n"]
        resp.headers["HX-Trigger"] = json.dumps({
            "wytchr:favorited": {
                "favorited": favorited,
                "title": title,
                "webhook_count": webhook_count,
            }
        })
        return resp
    return jsonify({"ok": True, "favorited": favorited})


@app.post("/videos/<video_id>/fetch-description")
@auth_required
async def fetch_video_description(video_id: str):
    """On-demand description fetch. Awaits the API call so the operator
    gets the updated card back in the same response."""
    db = await get_db()
    row = await db.execute(
        "SELECT video_id, url FROM videos WHERE video_id = ?", (video_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    desc = await _fetch_description(row["url"]) if row["url"] else None
    if desc:
        await db.execute(
            "UPDATE videos SET description = ? WHERE video_id = ?",
            (desc, video_id),
        )
        await db.commit()
        # description is selected into the tag-filtered slice payload.
        await _invalidate_video(db, video_id)
    if request.headers.get("HX-Request"):
        return await _render_card(video_id)
    return jsonify({"ok": bool(desc), "description": desc})


@app.get("/favorites")
@auth_required
async def favorites_page():
    db = await get_db()
    videos = await db.execute(
        """SELECT video_id, title, duration, upload_date, thumbnail_url, url,
                  status, channel_name, favorited_at
             FROM videos
            WHERE favorited_at IS NOT NULL
         ORDER BY favorited_at DESC""",
    ).fetchall()
    video_tags_map = await _video_tags_map(db, [v["video_id"] for v in videos])
    return await render_template(
        "favorites.html",
        videos=videos,
        video_tags_map=video_tags_map,
    )


@app.get("/webhooks")
@auth_required
async def webhooks_admin():
    db = await get_db()
    hooks = await db.execute(
        "SELECT id, name, url, event, enabled, bearer_token, created_at, last_fired_at, last_status, last_error FROM webhooks ORDER BY id"
    ).fetchall()
    always_favs = await _get_setting(db, "always_add_channel_to_favs") == "1"
    return await render_template("webhooks.html", hooks=hooks, always_favs=always_favs)


@app.post("/webhooks")
@auth_required
async def webhooks_create():
    f = await request.form
    name = (f.get("name") or "").strip()
    url = (f.get("url") or "").strip()
    event = (f.get("event") or "video.favorited").strip()
    bearer = (f.get("bearer_token") or "").strip() or None
    if not name or not url:
        return ("name and url are required", 400)
    if not (url.startswith("http://") or url.startswith("https://")):
        return ("url must be http(s)", 400)
    db = await get_db()
    await db.execute(
        "INSERT INTO webhooks (name, url, event, bearer_token, created_at) VALUES (?, ?, ?, ?, ?)",
        (name, url, event, bearer, int(time.time())),
    )
    await db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/always-add-favs")
@auth_required
async def webhooks_set_always_add_favs():
    f = await request.form
    db = await get_db()
    await _set_setting(
        db, "always_add_channel_to_favs", "1" if f.get("enabled") else "0"
    )
    await db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/toggle")
@auth_required
async def webhooks_toggle(hook_id: int):
    db = await get_db()
    await db.execute("UPDATE webhooks SET enabled = 1 - enabled WHERE id = ?", (hook_id,))
    await db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/delete")
@auth_required
async def webhooks_delete(hook_id: int):
    db = await get_db()
    await db.execute("DELETE FROM webhooks WHERE id = ?", (hook_id,))
    await db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/test")
@auth_required
async def webhooks_test(hook_id: int):
    db = await get_db()
    row = await db.execute(
        "SELECT event FROM webhooks WHERE id = ?", (hook_id,)
    ).fetchone()
    if not row:
        return ("not found", 404)
    asyncio.create_task(_fire_webhooks(
        row["event"],
        {
            "id": "test", "title": "wytchr test event",
            "url": "https://wytchr.example/test",
            "channel": "wytchr", "thumbnail_url": None,
            "duration": 0, "upload_date": None,
            "description": "test description from wytchr",
            "notes": "test description from wytchr",
            "test": True,
        },
        hook_id=hook_id,
    ))
    return redirect("/webhooks")


# --- Bootstrap --------------------------------------------------------

async def auto_mark_watched() -> dict:
    """Per-channel auto-mark-watched sweep.

    Reads channel_settings.auto_watched_days as the per-channel window
    (NULL = never auto-watch). For each channel with a window set, any
    video whose seen_at predates now − window AND has watched_at IS NULL
    AND status = 'new' flips to status='watched',
    watched_at=now(). Manual marks aren't overwritten — they already
    have watched_at populated.
    """
    summary: dict = {"channels": 0, "marked": 0}
    swept: list[str] = []
    now = int(time.time())
    async with standalone_db() as db:
        rows = await db.execute(
            """SELECT channel_name, auto_watched_days
                 FROM channel_settings
                WHERE auto_watched_days IS NOT NULL AND auto_watched_days > 0"""
        ).fetchall()
        for r in rows:
            cutoff = now - (int(r["auto_watched_days"]) * 86400)
            cur = await db.execute(
                """UPDATE videos
                      SET status = 'watched',
                          status_changed_at = ?,
                          watched_at = ?
                    WHERE channel_name = ?
                      AND watched_at IS NULL
                      AND status = 'new'
                      AND seen_at < ?""",
                (now, now, r["channel_name"], cutoff),
            )
            marked = cur.rowcount or 0
            summary["channels"] += 1
            summary["marked"] += marked
            if marked:
                swept.append(r["channel_name"])
        await db.commit()
    # After the commit, so a reader that misses mid-sweep can't refill
    # the slice from the pre-update rows.
    for channel_name in swept:
        await _invalidate_channel(channel_name)
    if summary["marked"]:
        print(f"auto_mark_watched: {summary}", file=sys.stderr, flush=True)
    return summary


@app.before_serving
async def _startup():
    global _http_client, _scheduler, _valkey
    _http_client = httpx.AsyncClient()
    # from_url() is lazy — it builds a pool without dialling, so an
    # unreachable Valkey surfaces per-operation (and degrades via
    # _cache_degrade) rather than blocking boot. decode_responses keeps
    # scan_iter handing back str keys for the f-string in unlink.
    _valkey = valkey.Valkey.from_url(
        VALKEY_URL,
        decode_responses=True,
        socket_timeout=CACHE_SOCKET_TIMEOUT,
        socket_connect_timeout=CACHE_SOCKET_TIMEOUT,
    )
    await init_db()
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        poll_all, "interval", minutes=POLL_INTERVAL_MINUTES, next_run_time=None
    )
    _scheduler.add_job(
        auto_mark_watched, "interval", hours=1, next_run_time=None
    )
    _scheduler.start()


@app.after_serving
async def _shutdown():
    global _http_client, _scheduler, _valkey
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None
    if _valkey is not None:
        try:
            await _valkey.aclose()
        except Exception as exc:  # noqa: BLE001 — shutdown must not raise
            _cache_degrade("close", exc)
        _valkey = None


if __name__ == "__main__":
    # Dev only — production runs hypercorn from the Dockerfile CMD.
    app.run(host="0.0.0.0", port=int(os.environ.get("FLASK_RUN_PORT", 5000)))
