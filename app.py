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

__version__ = "0.14.0"

API_TOKEN = os.environ.get("API_TOKEN", "")
# YouTube Data API v3 key. Required — drives channel resolution, the
# poll loop, and description enrichment.
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))
POLL_LIMIT = int(os.environ.get("POLL_LIMIT", "30"))

if not API_TOKEN:
    print("FATAL: API_TOKEN env var must be set", file=sys.stderr)
    sys.exit(1)

if not DATABASE_URL:
    print("FATAL: DATABASE_URL env var must be set (Postgres URI from install.sh)", file=sys.stderr)
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

# Process-wide singletons. Both are created in @app.before_serving so
# they bind to the running event loop, and torn down in
# @app.after_serving. Anywhere outside a request that needs HTTP or
# the scheduler should reach for these.
_http_client: httpx.AsyncClient | None = None
_scheduler: AsyncIOScheduler | None = None


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

    if show_hidden:
        status_filter = ("hidden",)
    else:
        # wytchr is a selector, not a library view: only show videos
        # the operator hasn't acted on yet.
        status_filter = ("new",)
    status_placeholders = ",".join("?" * len(status_filter))

    settings_map = await _channel_settings_map(db)

    PER_CHANNEL_LIMIT = 20

    columns = []
    for ch in channels:
        cs = settings_map.get(ch["name"], _CHANNEL_SETTINGS_DEFAULTS)
        if cs["hide_channel"] and not show_hidden:
            continue

        per_channel_limit = PER_CHANNEL_LIMIT

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

        window_clause = ""
        window_args: tuple = ()

        if tag_filter_id is not None:
            channel_has_tag = await db.execute(
                "SELECT 1 FROM channel_tags WHERE channel_name = ? AND tag_id = ?",
                (ch["name"], tag_filter_id),
            ).fetchone()
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
                    (ch["name"], *status_filter, *window_args, per_channel_limit),
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
                    (ch["name"], *status_filter, tag_filter_id, *window_args, per_channel_limit),
                ).fetchall()
                if not videos:
                    continue
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
                (ch["name"], *status_filter, *window_args, per_channel_limit),
            ).fetchall()

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

        ch_dict = dict(ch)
        ch_dict["display_name"] = cs["display_name"] or ch["name"]
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
    for section in sections:
        section["columns"].sort(key=lambda c: 0 if c["videos"] else 1)
    if tag_filter_name:
        sections = [s for s in sections if s["columns"]]

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

    totals = {
        "channels": len(channels),
        "videos": sum(sum(c["counts"].values()) for c in columns),
        "new": sum(c["counts"].get("new", 0) for c in columns),
        "watched": sum(c["counts"].get("watched", 0) for c in columns),
    }
    return await render_template(
        "_board.html",
        columns=columns,
        sections=sections,
        show_hidden=show_hidden,
        totals=totals,
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
    if request.headers.get("HX-Request"):
        return ("", 200)
    return jsonify({"ok": True})


@app.post("/channels/<channel_name>/mark-watched")
@auth_required
async def mark_channel_watched(channel_name: str):
    """Bulk-flip every actionable video in the channel to 'watched'."""
    db = await get_db()
    if not await db.execute("SELECT 1 FROM channels WHERE name = ?", (channel_name,)).fetchone():
        return ("channel not found", 404)
    now = int(time.time())
    cur = await db.execute(
        """UPDATE videos
              SET status = 'watched', status_changed_at = ?,
                  watched_at = COALESCE(watched_at, ?)
            WHERE channel_name = ?
              AND status = 'new'""",
        (now, now, channel_name),
    )
    await db.commit()
    marked = cur.rowcount or 0
    if request.headers.get("HX-Request"):
        body = await board_partial()
        resp = await make_response(body)
        resp.headers["HX-Trigger"] = json.dumps({
            "wytchr:channel-watched": {"channel": channel_name, "marked": marked}
        })
        return resp
    return jsonify({"ok": True, "marked": marked})


@app.post("/videos/<video_id>/unhide")
@auth_required
async def unhide_video(video_id: str):
    db = await get_db()
    await db.execute(
        "UPDATE videos SET status = 'new', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    await db.commit()
    if request.headers.get("HX-Request"):
        return await _render_card(video_id)
    return jsonify({"ok": True})


async def _render_card(video_id: str):
    db = await get_db()
    row = await db.execute(
        """SELECT video_id, title, duration, upload_date, thumbnail_url, url, status, channel_name, favorited_at, description
             FROM videos WHERE video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return ("", 404)
    return await render_template("_card.html", v=row)


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
        await db.commit()
    if summary["marked"]:
        print(f"auto_mark_watched: {summary}", file=sys.stderr, flush=True)
    return summary


@app.before_serving
async def _startup():
    global _http_client, _scheduler
    _http_client = httpx.AsyncClient()
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
    global _http_client, _scheduler
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
        _scheduler = None
    if _http_client is not None:
        await _http_client.aclose()
        _http_client = None


if __name__ == "__main__":
    # Dev only — production runs hypercorn from the Dockerfile CMD.
    app.run(host="0.0.0.0", port=int(os.environ.get("FLASK_RUN_PORT", 5000)))
