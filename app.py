"""wytchr — manual-download UI on top of ytdl-sub-api.

Polls each channel registered with ytdl-sub-api, surfaces recent
uploads in a per-channel column board, and posts a one-off download
to ytdl-sub-api when the user clicks. PostgreSQL (Aiven) is the only
state. Quart + asyncio: every route is async, DB is psycopg.AsyncConnection,
HTTP calls are httpx.AsyncClient. The yt-dlp subprocess is the one
intentionally-sync island — wrapped in asyncio.to_thread at the
single call site.
"""
from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from functools import wraps

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

__version__ = "0.11.0"

API_TOKEN = os.environ.get("API_TOKEN", "")
YTDL_SUB_API_URL = os.environ.get("YTDL_SUB_API_URL", "http://ytdl-sub-api:5000").rstrip("/")
YTDL_SUB_API_TOKEN = os.environ.get("YTDL_SUB_API_TOKEN", API_TOKEN)
# YouTube Data API v3 key. Currently optional (fetches video
# descriptions for webhook payloads). Becomes REQUIRED once the upcoming
# pivot lands — wytchr is moving channel resolution and upload polling
# off ytdl-sub-api onto the YouTube Data API.
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))
POLL_LIMIT = int(os.environ.get("POLL_LIMIT", "30"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "1800"))

if not API_TOKEN:
    print("FATAL: API_TOKEN env var must be set", file=sys.stderr)
    sys.exit(1)

if not DATABASE_URL:
    print("FATAL: DATABASE_URL env var must be set (Postgres URI from install.sh)", file=sys.stderr)
    sys.exit(1)

if not YOUTUBE_API_KEY:
    print(
        "WARNING: YOUTUBE_API_KEY is unset. Description enrichment is disabled, and "
        "future versions will require this key for channel resolution and upload polling.",
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
  last_output TEXT,
  seen_at BIGINT NOT NULL,
  status_changed_at BIGINT NOT NULL,
  favorited_at BIGINT,
  description TEXT
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
  date_range TEXT,
  max_files INTEGER,
  include_shorts INTEGER NOT NULL DEFAULT 0,
  hide_channel INTEGER NOT NULL DEFAULT 0,
  auto_watched_days INTEGER,
  title_include TEXT,
  title_exclude TEXT,
  include_members_only INTEGER NOT NULL DEFAULT 0,
  updated_at BIGINT NOT NULL
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
        await db.execute(
            "ALTER TABLE channel_settings ADD COLUMN IF NOT EXISTS "
            "include_members_only INTEGER NOT NULL DEFAULT 0"
        )


# --- Tags -------------------------------------------------------------

# yt-dlp's --flat-playlist sets `availability` on entries that are
# gated. We poll-skip these so they never enter the DB; the operator
# can't act on them anyway and they'd just be visual noise.
# `subscriber_only` is the channel-membership tier — split out so the
# per-channel `include_members_only` toggle can opt back in for
# channels where the operator is actually a member.
MEMBERS_ONLY_AVAILABILITY = frozenset({"subscriber_only"})
SKIP_AVAILABILITY = frozenset({"premium_only", "needs_auth"}) | MEMBERS_ONLY_AVAILABILITY


_CHANNEL_SETTINGS_DEFAULTS = {
    "display_name": None,
    "date_range": None,
    "max_files": None,
    "include_shorts": 0,
    "hide_channel": 0,
    "auto_watched_days": None,
    "title_include": None,
    "title_exclude": None,
    "include_members_only": 0,
}


async def _channel_settings_map(db: AsyncPgConn) -> dict[str, dict]:
    """Fetch every channel's settings row into {name: dict}. Channels
    without a row return defaults via .get(name, _CHANNEL_SETTINGS_DEFAULTS)
    at the call site."""
    rows = await db.execute(
        """SELECT channel_name, display_name, date_range, max_files,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude, include_members_only
             FROM channel_settings"""
    ).fetchall()
    out: dict[str, dict] = {}
    for r in rows:
        out[r["channel_name"]] = {
            "display_name": r["display_name"],
            "date_range": r["date_range"],
            "max_files": r["max_files"],
            "include_shorts": int(r["include_shorts"] or 0),
            "hide_channel": int(r["hide_channel"] or 0),
            "auto_watched_days": r["auto_watched_days"],
            "title_include": r["title_include"],
            "title_exclude": r["title_exclude"],
            "include_members_only": int(r["include_members_only"] or 0),
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


def _is_short(entry: dict) -> bool:
    # yt-dlp flat-playlist puts shorts under a `/shorts/<id>` URL. The
    # ie_key is "Youtube" for both, so the URL is the cleanest signal.
    for key in ("url", "webpage_url", "original_url"):
        u = entry.get(key)
        if isinstance(u, str) and "/shorts/" in u:
            return True
    return False


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


# --- ytdl-sub-api client ---------------------------------------------

def _api_headers() -> dict:
    return {"Authorization": f"Bearer {YTDL_SUB_API_TOKEN}"}


def _client() -> httpx.AsyncClient:
    """Return the process-wide AsyncClient. before_serving must have run."""
    if _http_client is None:
        # Defensive: ad-hoc fallback if someone calls this outside the
        # serving lifecycle (tests, scripts). Caller leaks the client;
        # acceptable for the one-off path.
        return httpx.AsyncClient()
    return _http_client


async def fetch_channels_from_api() -> list[dict]:
    """Pull current channel registry from ytdl-sub-api."""
    r = await _client().get(
        f"{YTDL_SUB_API_URL}/channels", headers=_api_headers(), timeout=15
    )
    r.raise_for_status()
    return r.json().get("channels", [])


# Profile cache. /presets is read on every /board fetch (sub-second
# response, but the network round-trip adds up at the 30s auto-poll
# cadence). 60s TTL is good enough — when the operator edits a profile
# via the admin page, the page itself force-refreshes.
_PRESETS_CACHE: dict = {"data": None, "fetched_at": 0.0}
_PRESETS_TTL = 60.0


async def fetch_presets(force: bool = False) -> dict:
    now = time.time()
    if not force and _PRESETS_CACHE["data"] is not None and (now - _PRESETS_CACHE["fetched_at"]) < _PRESETS_TTL:
        return _PRESETS_CACHE["data"]
    try:
        r = await _client().get(
            f"{YTDL_SUB_API_URL}/presets", headers=_api_headers(), timeout=10
        )
        r.raise_for_status()
        data = r.json()
    except Exception:  # noqa: BLE001
        return _PRESETS_CACHE["data"] or {"profiles": [], "profile_details": {}}
    _PRESETS_CACHE["data"] = data
    _PRESETS_CACHE["fetched_at"] = now
    return data


_DATE_RANGE_RE = re.compile(r"(\d+)\s*(day|week|month|year)s?", re.IGNORECASE)
_DATE_UNITS = {"day": 1, "week": 7, "month": 30, "year": 365}


def _parse_date_range(s: str | None) -> int | None:
    """ytdl-sub date_range like '7days' / '2weeks' / '6months' / '1year'.
    Returns the count in days, or None if unparseable / empty."""
    if not s:
        return None
    m = _DATE_RANGE_RE.match(str(s).strip())
    if not m:
        return None
    return int(m.group(1)) * _DATE_UNITS[m.group(2).lower()]


def _resolve_profile(preset_str: str | None, profile_details: dict) -> str | None:
    """Right-to-left scan of the chain for the first part that names a
    user-defined profile."""
    if not preset_str:
        return None
    parts = [p.strip() for p in preset_str.split("|") if p.strip()]
    for part in reversed(parts):
        if part in profile_details:
            return part
    return None


def _channel_window(preset_str: str | None, profile_details: dict) -> tuple[int | None, int | None]:
    """Resolve the channel's display window from its preset chain.

    Right-to-left because the most-specific profile is at the tail and
    should win over earlier ones. Returns (max_age_days, max_files);
    either may be None if not set by any profile in the chain.
    """
    if not preset_str:
        return None, None
    parts = [p.strip() for p in preset_str.split("|") if p.strip()]
    for part in reversed(parts):
        profile = profile_details.get(part)
        if not profile:
            continue
        overrides = profile.get("overrides") or {}
        days = _parse_date_range(overrides.get("only_recent_date_range"))
        max_files = overrides.get("only_recent_max_files")
        try:
            max_files = int(max_files) if max_files is not None else None
        except (TypeError, ValueError):
            max_files = None
        if days is not None or max_files is not None:
            return days, max_files
    return None, None


async def request_download(url: str, preset: str) -> tuple[int, str]:
    """POST /videos to ytdl-sub-api. Returns (exit_code, output_tail)."""
    try:
        r = await _client().post(
            f"{YTDL_SUB_API_URL}/videos",
            json={"url": url, "preset": preset},
            headers=_api_headers(),
            timeout=DOWNLOAD_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        return -1, f"network error: {exc}"
    if r.status_code == 404:
        return -1, "ytdl-sub-api /videos endpoint not deployed yet (apply the upstream patch in RUNBOOKS/phase-4-9-wytchr.md)"
    try:
        body = r.json()
    except ValueError:
        return r.status_code, r.text[-2000:]
    if r.status_code >= 400 and "error" in body:
        return body.get("exit_code", r.status_code), body["error"]
    return body.get("exit_code", r.status_code), (body.get("output_tail") or "")[-2000:]


# --- Polling ----------------------------------------------------------

def _flat_listing_sync(url: str, limit: int) -> list[dict]:
    """yt-dlp --flat-playlist --dump-json. One video JSON per line.

    Stays synchronous on purpose — the user's call: every other I/O
    path is asyncio-native, but spawning yt-dlp is a thread-pool job
    via asyncio.to_thread. Reasons: yt-dlp's async story is third-party
    and immature; subprocess.run is rock-solid and the throttling
    config already lives in the yt-dlp extractor args.
    """
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        "--ignore-errors",
        # `youtubetab:approximate_date` makes the channel-tab extractor
        # surface relative dates ("2 weeks ago") as a `timestamp` field
        # on flat-playlist entries.
        "--extractor-args", "youtubetab:approximate_date",
        "-I", f":{limit}",
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    out: list[dict] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    if not out and proc.returncode != 0:
        raise RuntimeError(proc.stderr.strip()[-500:] or f"yt-dlp exit {proc.returncode}")
    return out


async def _flat_listing(url: str, limit: int) -> list[dict]:
    return await asyncio.to_thread(_flat_listing_sync, url, limit)


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


def _best_thumbnail(entry: dict) -> str | None:
    """Pick a usable thumbnail URL from yt-dlp's flat-playlist entry."""
    thumbs = entry.get("thumbnails") or []
    if isinstance(thumbs, list) and thumbs:
        return thumbs[-1].get("url")
    if entry.get("thumbnail"):
        return entry["thumbnail"]
    vid = entry.get("id")
    if vid:
        return f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg"
    return None


async def poll_channel(db: AsyncPgConn, name: str, url: str) -> tuple[int, str | None]:
    """Returns (new_video_count, error_message)."""
    now = int(time.time())
    try:
        entries = await _flat_listing(url, POLL_LIMIT)
    except Exception as exc:  # noqa: BLE001
        await db.execute(
            "UPDATE channels SET last_polled_at = ?, last_error = ? WHERE name = ?",
            (now, str(exc)[:500], name),
        )
        return 0, str(exc)
    # Per-channel opt-in for members-only ingest.
    row = await db.execute(
        "SELECT include_members_only FROM channel_settings WHERE channel_name = ?",
        (name,),
    ).fetchone()
    include_members_only = bool(row and row["include_members_only"])
    new_count = 0
    for e in entries:
        vid = e.get("id")
        if not vid:
            continue
        avail = e.get("availability")
        if avail in MEMBERS_ONLY_AVAILABILITY:
            if not include_members_only:
                continue
        elif avail in SKIP_AVAILABILITY:
            continue
        if _is_short(e):
            continue
        existing = await db.execute(
            "SELECT status FROM videos WHERE video_id = ?", (vid,)
        ).fetchone()
        title = e.get("title")
        duration = int(e["duration"]) if isinstance(e.get("duration"), (int, float)) else None
        upload_date = e.get("upload_date")
        if not upload_date:
            ts = e.get("timestamp")
            if isinstance(ts, (int, float)) and ts > 0:
                from datetime import datetime, timezone
                upload_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")
        thumb = _best_thumbnail(e)
        watch_url = e.get("url") or e.get("webpage_url") or f"https://www.youtube.com/watch?v={vid}"
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


async def sync_channels_from_api(db: AsyncPgConn) -> None:
    """Mirror ytdl-sub-api's channel list into our local table."""
    remote = await fetch_channels_from_api()
    seen = set()
    for c in remote:
        name = c.get("name")
        url = c.get("url")
        # ytdl-sub-api returns `preset` as a string for Shape-1 (chained-
        # preset block) channels, but as a list for Shape-2 (standalone
        # subscription with inline preset chain). Coerce to a " | "-joined
        # string — matches the chained-preset key format used by Shape-1
        # entries.
        raw_preset = c.get("preset")
        if isinstance(raw_preset, list):
            preset = " | ".join(str(p) for p in raw_preset)
        else:
            preset = raw_preset or ""
        if not name or not url:
            continue
        seen.add(name)
        await db.execute(
            """INSERT INTO channels (name, url, preset)
               VALUES (?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET url = excluded.url, preset = excluded.preset""",
            (name, url, preset),
        )
    if seen:
        placeholders = ",".join("?" * len(seen))
        await db.execute(
            f"DELETE FROM channels WHERE name NOT IN ({placeholders})",
            tuple(seen),
        )


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
    """Sync channel list, then poll each channel. Caller owns the run-flag."""
    started = time.time()
    summary: dict = {"channels": 0, "new_videos": 0, "errors": []}
    try:
        async with standalone_db() as db:
            try:
                await sync_channels_from_api(db)
            except Exception as exc:  # noqa: BLE001
                summary["errors"].append(f"sync: {exc}")
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
    presets_data = await fetch_presets()
    profiles = sorted((presets_data.get("profile_details") or {}).keys())
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
        status_filter = ("new", "queued", "downloading", "failed")
    status_placeholders = ",".join("?" * len(status_filter))

    presets_data = await fetch_presets()
    profile_details = presets_data.get("profile_details") or {}
    settings_map = await _channel_settings_map(db)
    from datetime import datetime, timedelta, timezone
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())

    columns = []
    for ch in channels:
        cs = settings_map.get(ch["name"], _CHANNEL_SETTINGS_DEFAULTS)
        if cs["hide_channel"] and not show_hidden:
            continue
        prof_days, prof_max = _channel_window(ch["preset"], profile_details)
        cs_days = _parse_date_range(cs["date_range"]) if cs["date_range"] else None
        days = cs_days if cs_days is not None else prof_days
        cs_max = cs["max_files"]
        max_files = cs_max if (isinstance(cs_max, int) and cs_max > 0) else prof_max

        aw = cs["auto_watched_days"]
        if isinstance(aw, int) and aw > 0:
            aw_cutoff = (now_dt - timedelta(days=aw)).strftime("%Y%m%d")
            await db.execute(
                """UPDATE videos
                      SET status = 'watched', status_changed_at = ?
                    WHERE channel_name = ?
                      AND status IN ('new', 'failed')
                      AND upload_date IS NOT NULL
                      AND upload_date < ?""",
                (now_ts, ch["name"], aw_cutoff),
            )

        cutoff = None
        if days is not None:
            cutoff = (now_dt - timedelta(days=days)).strftime("%Y%m%d")
        per_channel_limit = max_files if (max_files is not None and max_files > 0) else 20

        shorts_clause = "" if cs["include_shorts"] else " AND v.url NOT LIKE '%/shorts/%'"
        shorts_clause_count = "" if cs["include_shorts"] else " AND url NOT LIKE '%/shorts/%'"
        title_inc_re = _compile_title_re(cs["title_include"])
        title_exc_re = _compile_title_re(cs["title_exclude"])

        if cutoff and not show_hidden:
            count_rows = await db.execute(
                f"""SELECT status, COUNT(*) AS n FROM videos
                     WHERE channel_name = ?
                       {shorts_clause_count}
                       AND upload_date IS NOT NULL
                       AND upload_date >= ?
                  GROUP BY status""",
                (ch["name"], cutoff),
            ).fetchall()
        else:
            count_rows = await db.execute(
                f"SELECT status, COUNT(*) AS n FROM videos WHERE channel_name = ? {shorts_clause_count} GROUP BY status",
                (ch["name"],),
            ).fetchall()
        counts = {row["status"]: row["n"] for row in count_rows}
        actionable = counts.get("new", 0) + counts.get("failed", 0)

        window_clause = ""
        window_args: tuple = ()
        if cutoff and not show_hidden:
            window_clause = " AND v.upload_date IS NOT NULL AND v.upload_date >= ?"
            window_args = (cutoff,)

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
    for profile_name in profile_details:
        section_idx[profile_name] = len(sections)
        sections.append({"profile": profile_name, "columns": []})
    for col in columns:
        profile_name = _resolve_profile(col["channel"]["preset"], profile_details)
        if profile_name and profile_name in section_idx:
            sections[section_idx[profile_name]]["columns"].append(col)
        else:
            if "" not in section_idx:
                section_idx[""] = len(sections)
                sections.append({"profile": "", "columns": []})
            sections[section_idx[""]]["columns"].append(col)
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
        "failed": sum(c["counts"].get("failed", 0) for c in columns),
        "queued": sum(c["counts"].get("queued", 0) + c["counts"].get("downloading", 0) for c in columns),
        "done": sum(c["counts"].get("done", 0) for c in columns),
        "watched": sum(c["counts"].get("watched", 0) for c in columns),
    }
    return await render_template(
        "_board.html",
        columns=columns,
        sections=sections,
        base_preset=presets_data.get("base_preset", ""),
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
    presets_data = await fetch_presets()
    profile_details = presets_data.get("profile_details") or {}
    profiles = sorted(profile_details.keys())
    return await render_template(
        "channel_add.html",
        profiles=profiles,
        base_preset=presets_data.get("base_preset", ""),
        error=request.args.get("error"),
        prefill_url=request.args.get("url", ""),
    )


_NAME_RE = re.compile(r"[^A-Za-z0-9_-]+")


def _channel_id_from_url(url: str) -> str:
    """Last path segment without an `@` prefix, slugified.

    Used as the fallback ytdl-sub-api channel id when the operator
    doesn't provide one and the resolver couldn't extract a handle.
    """
    tail = (url or "").rstrip("/").rsplit("/", 1)[-1]
    return _NAME_RE.sub("-", tail.lstrip("@")).strip("-")


@app.post("/channels/add")
@auth_required
async def channel_add():
    f = await request.form
    raw_url = (f.get("url") or "").strip()
    override_name = (f.get("name") or "").strip()
    profile = (f.get("profile") or "").strip()
    if not raw_url:
        return redirect("/channels/add?error=url+is+required")

    try:
        resolved = await _resolve_channel(raw_url)
    except Exception as exc:  # noqa: BLE001
        msg = str(exc)[:200].replace(" ", "+")
        return redirect(f"/channels/add?error=could+not+resolve:+{msg}&url={raw_url}")

    channel_url = resolved["channel_url"]
    name = override_name or resolved["handle"] or _channel_id_from_url(channel_url)
    name = _NAME_RE.sub("-", name).strip("-")
    if not name:
        return redirect(f"/channels/add?error=could+not+derive+a+name&url={raw_url}")

    payload: dict = {"url": channel_url, "name": name}
    if profile:
        payload["profile"] = profile
    try:
        r = await _client().post(
            f"{YTDL_SUB_API_URL}/channels",
            headers=_api_headers(), json=payload, timeout=20,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/channels/add?error=upstream+unreachable:+{exc}&url={raw_url}")
    if r.status_code != 201:
        try:
            err = r.json().get("error", r.text[:200])
        except Exception:  # noqa: BLE001
            err = r.text[:200]
        err = str(err)[:200].replace(" ", "+")
        return redirect(f"/channels/add?error={err}&url={raw_url}")

    # Mirror the upstream registry into our DB and kick a poll so the
    # new channel shows up on the board without waiting for the next
    # scheduler tick.
    async with standalone_db() as db:
        try:
            await sync_channels_from_api(db)
        except Exception:  # noqa: BLE001
            pass
    await _start_poll_async()
    return redirect("/")


@app.post("/channels/<channel_name>/profile")
@auth_required
async def channel_change_profile(channel_name: str):
    """Reassign a channel's profile (DELETE + POST upstream)."""
    body = await request.get_json(silent=True) or {}
    new_profile = (body.get("profile") or "").strip()
    db = await get_db()
    row = await db.execute(
        "SELECT url FROM channels WHERE name = ?", (channel_name,)
    ).fetchone()
    if not row:
        return jsonify({"error": "channel not found in wytchr DB"}), 404
    url = row["url"]

    try:
        d = await _client().delete(
            f"{YTDL_SUB_API_URL}/channels/{channel_name}",
            headers=_api_headers(), timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"delete: {exc}"}), 502
    if d.status_code not in (200, 404):
        return jsonify({"error": f"delete: {d.status_code} {d.text[:200]}"}), 502

    payload: dict = {"url": url, "name": channel_name}
    if new_profile:
        payload["profile"] = new_profile
    try:
        p = await _client().post(
            f"{YTDL_SUB_API_URL}/channels",
            headers=_api_headers(), json=payload, timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": f"post: {exc}", "stranded": True}), 502
    if p.status_code != 201:
        try:
            err = p.json().get("error", p.text[:200])
        except Exception:  # noqa: BLE001
            err = p.text[:200]
        return jsonify({"error": err, "stranded": True, "status": p.status_code}), 502

    presets_data = await fetch_presets(force=True)
    base = presets_data.get("base_preset", "")
    new_preset = f"{base} | {new_profile}" if new_profile else base
    await db.execute(
        "UPDATE channels SET preset = ? WHERE name = ?",
        (new_preset, channel_name),
    )
    await db.commit()
    return jsonify({"ok": True, "preset": new_preset})


# --- Per-channel settings ---------------------------------------------

async def _get_channel_settings_row(db: AsyncPgConn, channel_name: str) -> dict:
    r = await db.execute(
        """SELECT display_name, date_range, max_files,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude, include_members_only
             FROM channel_settings WHERE channel_name = ?""",
        (channel_name,),
    ).fetchone()
    if r is None:
        return dict(_CHANNEL_SETTINGS_DEFAULTS)
    return {
        "display_name": r["display_name"],
        "date_range": r["date_range"],
        "max_files": r["max_files"],
        "include_shorts": int(r["include_shorts"] or 0),
        "hide_channel": int(r["hide_channel"] or 0),
        "auto_watched_days": r["auto_watched_days"],
        "title_include": r["title_include"],
        "title_exclude": r["title_exclude"],
        "include_members_only": int(r["include_members_only"] or 0),
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
    presets_data = await fetch_presets()
    profile_details = presets_data.get("profile_details") or {}
    prof_days, prof_max = _channel_window(ch["preset"], profile_details)
    return await render_template(
        "channel_settings.html",
        channel=ch,
        settings=settings,
        profile_days=prof_days,
        profile_max=prof_max,
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
        "date_range": _opt_str("date_range"),
        "max_files": _opt_int("max_files"),
        "include_shorts": 1 if f.get("include_shorts") else 0,
        "hide_channel": 1 if f.get("hide_channel") else 0,
        "auto_watched_days": _opt_int("auto_watched_days"),
        "title_include": _opt_str("title_include"),
        "title_exclude": _opt_str("title_exclude"),
        "include_members_only": 1 if f.get("include_members_only") else 0,
    }
    for key in ("title_include", "title_exclude"):
        if payload[key] and _compile_title_re(payload[key]) is None:
            return (f"invalid regex for {key}: {payload[key]}", 400)
    if payload["date_range"] and _parse_date_range(payload["date_range"]) is None:
        return ("invalid date_range (try '7days', '2weeks', '6months', '1year')", 400)

    await db.execute(
        """INSERT INTO channel_settings
             (channel_name, display_name, date_range, max_files,
              include_shorts, hide_channel, auto_watched_days,
              title_include, title_exclude, include_members_only, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(channel_name) DO UPDATE SET
             display_name = excluded.display_name,
             date_range = excluded.date_range,
             max_files = excluded.max_files,
             include_shorts = excluded.include_shorts,
             hide_channel = excluded.hide_channel,
             auto_watched_days = excluded.auto_watched_days,
             title_include = excluded.title_include,
             title_exclude = excluded.title_exclude,
             include_members_only = excluded.include_members_only,
             updated_at = excluded.updated_at""",
        (
            channel_name,
            payload["display_name"],
            payload["date_range"],
            payload["max_files"],
            payload["include_shorts"],
            payload["hide_channel"],
            payload["auto_watched_days"],
            payload["title_include"],
            payload["title_exclude"],
            payload["include_members_only"],
            int(time.time()),
        ),
    )
    await db.commit()
    return redirect(f"/channels/{channel_name}/settings?saved=1")


# --- Presets admin (proxy to ytdl-sub-api) ----------------------------

def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").split(",") if p.strip()]


@app.get("/presets")
@auth_required
async def presets_admin():
    data = await fetch_presets(force=True)
    profile_details = data.get("profile_details") or {}
    base_preset = data.get("base_preset", "")
    profiles = []
    for name, body in sorted(profile_details.items()):
        overrides = body.get("overrides") or {}
        profiles.append({
            "name": name,
            "parents": body.get("parents") or [],
            "overrides": overrides,
            "date_range": overrides.get("only_recent_date_range") or "",
            "max_files": overrides.get("only_recent_max_files"),
            "extra": {k: v for k, v in overrides.items() if k not in ("only_recent_date_range", "only_recent_max_files")},
        })
    flash = request.args.get("flash") or ""
    flash_kind = request.args.get("kind") or "ok"
    return await render_template(
        "presets.html",
        profiles=profiles,
        base_preset=base_preset,
        flash=flash,
        flash_kind=flash_kind,
    )


async def _overrides_from_form() -> dict:
    """Reconstruct an overrides dict from the admin form."""
    out: dict = {}
    f = await request.form
    date_range = (f.get("date_range") or "").strip()
    if date_range:
        out["only_recent_date_range"] = date_range
    max_files_raw = (f.get("max_files") or "").strip()
    if max_files_raw:
        try:
            out["only_recent_max_files"] = int(max_files_raw)
        except ValueError:
            pass
    extra = f.get("extra") or ""
    for line in extra.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        try:
            out[k] = int(v)
        except ValueError:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return out


@app.post("/presets/create")
@auth_required
async def presets_create():
    f = await request.form
    name = (f.get("name") or "").strip()
    parents = _split_csv(f.get("parents") or "")
    overrides = await _overrides_from_form()
    payload = {"name": name, "parents": parents, "overrides": overrides}
    try:
        r = await _client().post(
            f"{YTDL_SUB_API_URL}/presets",
            headers=_api_headers(),
            json=payload,
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    await fetch_presets(force=True)
    if r.status_code == 201:
        return redirect(f"/presets?flash=created+{name}")
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return redirect(f"/presets?kind=err&flash={body.get('error', 'create+failed')}")


@app.post("/presets/<name>/update")
@auth_required
async def presets_update(name: str):
    f = await request.form
    parents = _split_csv(f.get("parents") or "")
    overrides = await _overrides_from_form()
    payload = {"parents": parents, "overrides": overrides}
    try:
        r = await _client().patch(
            f"{YTDL_SUB_API_URL}/presets/{name}",
            headers=_api_headers(),
            json=payload,
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    await fetch_presets(force=True)
    if r.status_code == 200:
        return redirect(f"/presets?flash=updated+{name}")
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return redirect(f"/presets?kind=err&flash={body.get('error', 'update+failed')}")


@app.post("/presets/<name>/delete")
@auth_required
async def presets_delete(name: str):
    try:
        r = await _client().delete(
            f"{YTDL_SUB_API_URL}/presets/{name}",
            headers=_api_headers(),
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    await fetch_presets(force=True)
    if r.status_code == 200:
        return redirect(f"/presets?flash=deleted+{name}")
    if r.status_code == 409:
        body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
        users = body.get("users") or []
        names = [u.get("name", "?") for u in users[:5]]
        more = "" if len(users) <= 5 else f"+{len(users)-5}+more"
        return redirect(f"/presets?kind=err&flash={name}+in+use+by+" + "+".join(names) + more)
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return redirect(f"/presets?kind=err&flash={body.get('error', 'delete+failed')}")


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


@app.post("/videos/<video_id>/download")
@auth_required
async def download_video(video_id: str):
    db = await get_db()
    row = await db.execute(
        """SELECT v.url, v.channel_name, c.preset
             FROM videos v
             JOIN channels c ON c.name = v.channel_name
            WHERE v.video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    now = int(time.time())
    await db.execute(
        "UPDATE videos SET status = 'queued', status_changed_at = ? WHERE video_id = ?",
        (now, video_id),
    )
    await db.commit()
    exit_code, output = await request_download(row["url"], row["preset"])
    final_status = "done" if exit_code == 0 else "failed"
    await db.execute(
        "UPDATE videos SET status = ?, last_output = ?, status_changed_at = ? WHERE video_id = ?",
        (final_status, output, int(time.time()), video_id),
    )
    await db.commit()
    if request.headers.get("HX-Request"):
        return await _render_card(video_id)
    return jsonify({"status": final_status, "exit_code": exit_code, "output": output})


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
    await db.execute(
        "UPDATE videos SET status = 'watched', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
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
              SET status = 'watched', status_changed_at = ?
            WHERE channel_name = ?
              AND status IN ('new', 'failed')""",
        (now, channel_name),
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
    return await render_template("webhooks.html", hooks=hooks)


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

@app.before_serving
async def _startup():
    global _http_client, _scheduler
    _http_client = httpx.AsyncClient()
    await init_db()
    _scheduler = AsyncIOScheduler(timezone="UTC")
    _scheduler.add_job(
        poll_all, "interval", minutes=POLL_INTERVAL_MINUTES, next_run_time=None
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
