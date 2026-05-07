"""wytchr — manual-download UI on top of ytdl-sub-api.

Polls each channel registered with ytdl-sub-api, surfaces recent
uploads in a per-channel column board, and posts a one-off download
to ytdl-sub-api when the user clicks. SQLite is the only state.
"""
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

import httpx
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, g, jsonify, redirect, render_template, request

API_TOKEN = os.environ.get("API_TOKEN", "")
YTDL_SUB_API_URL = os.environ.get("YTDL_SUB_API_URL", "http://ytdl-sub-api:5000").rstrip("/")
YTDL_SUB_API_TOKEN = os.environ.get("YTDL_SUB_API_TOKEN", API_TOKEN)
# YouTube Data API v3 key for fetching video descriptions. Optional —
# when unset, description-related fields stay NULL but everything else
# works. The flat-playlist polling path doesn't depend on it.
YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "")
DB_PATH = Path(os.environ.get("DB_PATH", "/data/wytchr.db"))
POLL_INTERVAL_MINUTES = int(os.environ.get("POLL_INTERVAL_MINUTES", "30"))
POLL_LIMIT = int(os.environ.get("POLL_LIMIT", "30"))
DOWNLOAD_TIMEOUT = int(os.environ.get("DOWNLOAD_TIMEOUT", "1800"))

if not API_TOKEN:
    print("FATAL: API_TOKEN env var must be set", file=sys.stderr)
    sys.exit(1)

DB_PATH.parent.mkdir(parents=True, exist_ok=True)

app = Flask(__name__)


# --- DB ---------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
  name TEXT PRIMARY KEY,
  url TEXT NOT NULL,
  preset TEXT NOT NULL,
  last_polled_at INTEGER,
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
  seen_at INTEGER NOT NULL,
  status_changed_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS videos_channel_status ON videos(channel_name, status);
CREATE INDEX IF NOT EXISTS videos_seen ON videos(seen_at DESC);

-- Outbound webhooks. Fired on `video.favorited` (toggle on); designed
-- to feed downstream services like all-my-favs.
CREATE TABLE IF NOT EXISTS webhooks (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  url TEXT NOT NULL,
  event TEXT NOT NULL DEFAULT 'video.favorited',
  enabled INTEGER NOT NULL DEFAULT 1,
  bearer_token TEXT,
  created_at INTEGER NOT NULL,
  last_fired_at INTEGER,
  last_status INTEGER,
  last_error TEXT
);
CREATE INDEX IF NOT EXISTS webhooks_event_enabled ON webhooks(event, enabled);

CREATE TABLE IF NOT EXISTS tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE COLLATE NOCASE
);
CREATE TABLE IF NOT EXISTS video_tags (
  video_id TEXT NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (video_id, tag_id),
  FOREIGN KEY (video_id) REFERENCES videos(video_id) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS channel_tags (
  channel_name TEXT NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (channel_name, tag_id),
  FOREIGN KEY (channel_name) REFERENCES channels(name) ON DELETE CASCADE,
  FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS video_tags_tag ON video_tags(tag_id);
CREATE INDEX IF NOT EXISTS channel_tags_tag ON channel_tags(tag_id);

-- Per-channel overrides. Every column NULLs to "inherit" — the profile
-- (via preset chain) provides the default. UI surfaces these as a form
-- on /channels/<name>/settings.
CREATE TABLE IF NOT EXISTS channel_settings (
  channel_name TEXT PRIMARY KEY,
  display_name TEXT,
  date_range TEXT,
  max_files INTEGER,
  include_shorts INTEGER NOT NULL DEFAULT 0,
  hide_channel INTEGER NOT NULL DEFAULT 0,
  auto_watched_days INTEGER,
  title_include TEXT,
  title_exclude TEXT,
  updated_at INTEGER NOT NULL,
  FOREIGN KEY (channel_name) REFERENCES channels(name) ON DELETE CASCADE
);
"""


def get_db() -> sqlite3.Connection:
    db = getattr(g, "_db", None)
    if db is None:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        g._db = db
    return db


@app.teardown_appcontext
def close_db(_exc):
    db = getattr(g, "_db", None)
    if db is not None:
        db.close()


@contextmanager
def standalone_db():
    """For background jobs running outside a request context."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")
    try:
        yield db
        db.commit()
    finally:
        db.close()


def init_db():
    with standalone_db() as db:
        db.executescript(SCHEMA)
        # ALTER-ADD-COLUMN migrations for older DBs. SQLite can't do
        # ADD COLUMN IF NOT EXISTS, so we introspect first.
        cols = {r[1] for r in db.execute("PRAGMA table_info(videos)")}
        if "favorited_at" not in cols:
            db.execute("ALTER TABLE videos ADD COLUMN favorited_at INTEGER")
            db.execute("CREATE INDEX IF NOT EXISTS videos_favorited ON videos(favorited_at DESC)")
        if "description" not in cols:
            db.execute("ALTER TABLE videos ADD COLUMN description TEXT")


# --- Tags -------------------------------------------------------------

# yt-dlp's --flat-playlist sets `availability` on entries that are
# gated. We poll-skip these so they never enter the DB; the operator
# can't act on them anyway and they'd just be visual noise.
SKIP_AVAILABILITY = frozenset({"subscriber_only", "premium_only", "needs_auth"})


_CHANNEL_SETTINGS_DEFAULTS = {
    "display_name": None,
    "date_range": None,
    "max_files": None,
    "include_shorts": 0,
    "hide_channel": 0,
    "auto_watched_days": None,
    "title_include": None,
    "title_exclude": None,
}


def _channel_settings_map(db: sqlite3.Connection) -> dict[str, dict]:
    """Fetch every channel's settings row into {name: dict}. Channels
    without a row return defaults via .get(name, _CHANNEL_SETTINGS_DEFAULTS)
    at the call site."""
    out: dict[str, dict] = {}
    for r in db.execute(
        """SELECT channel_name, display_name, date_range, max_files,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude
             FROM channel_settings"""
    ):
        out[r["channel_name"]] = {
            "display_name": r["display_name"],
            "date_range": r["date_range"],
            "max_files": r["max_files"],
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
            # `prefix:` with nothing after — drop the colon and treat
            # as a plain tag so we don't store a trailing-colon ghost.
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


def _upsert_tag(db: sqlite3.Connection, name: str) -> int | None:
    norm = _normalize_tag(name)
    if not norm:
        return None
    db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (norm,))
    row = db.execute("SELECT id FROM tags WHERE name = ?", (norm,)).fetchone()
    return row["id"] if row else None


def _video_tags_map(db: sqlite3.Connection, video_ids: list[str]) -> dict[str, list[str]]:
    if not video_ids:
        return {}
    placeholders = ",".join("?" * len(video_ids))
    rows = db.execute(
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


def _channel_tags_map(db: sqlite3.Connection, names: list[str]) -> dict[str, list[str]]:
    if not names:
        return {}
    placeholders = ",".join("?" * len(names))
    rows = db.execute(
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
    def wrapper(*a, **kw):
        if not _authed():
            if request.headers.get("HX-Request"):
                return ("unauthorized", 401)
            if request.method == "GET" and request.accept_mimetypes.accept_html:
                return redirect("/login")
            return jsonify({"error": "unauthorized"}), 401
        return fn(*a, **kw)
    return wrapper


# --- ytdl-sub-api client ---------------------------------------------

def _api_headers() -> dict:
    return {"Authorization": f"Bearer {YTDL_SUB_API_TOKEN}"}


def fetch_channels_from_api() -> list[dict]:
    """Pull current channel registry from ytdl-sub-api."""
    r = httpx.get(f"{YTDL_SUB_API_URL}/channels", headers=_api_headers(), timeout=15)
    r.raise_for_status()
    return r.json().get("channels", [])


# Profile cache. /presets is read on every /board fetch (sub-second
# response, but the network round-trip adds up at the 30s auto-poll
# cadence). 60s TTL is good enough — when the operator edits a profile
# via the admin page, the page itself force-refreshes.
_PRESETS_CACHE: dict = {"data": None, "fetched_at": 0.0}
_PRESETS_TTL = 60.0


def fetch_presets(force: bool = False) -> dict:
    now = time.time()
    if not force and _PRESETS_CACHE["data"] is not None and (now - _PRESETS_CACHE["fetched_at"]) < _PRESETS_TTL:
        return _PRESETS_CACHE["data"]
    try:
        r = httpx.get(f"{YTDL_SUB_API_URL}/presets", headers=_api_headers(), timeout=10)
        r.raise_for_status()
        data = r.json()
    except Exception:  # noqa: BLE001
        # Fall back to whatever we last saw, or an empty shell.
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
    user-defined profile. Returns None when nothing in the chain
    matches (e.g., Shape-2 entries whose chain is just the base + a
    built-in like `Only Recent`)."""
    if not preset_str:
        return None
    parts = [p.strip() for p in preset_str.split("|") if p.strip()]
    for part in reversed(parts):
        if part in profile_details:
            return part
    return None


def _channel_window(preset_str: str | None, profile_details: dict) -> tuple[int | None, int | None]:
    """Resolve the channel's display window from its preset chain.

    Walks the chain (joined with " | ") right-to-left and returns the
    first profile that has `only_recent_date_range` or
    `only_recent_max_files` overrides. Right-to-left because the most-
    specific profile (e.g. `daily`) is at the tail of the chain and
    should win over earlier ones.

    Returns (max_age_days, max_files); either may be None if not set
    by any profile in the chain.
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


def request_download(url: str, preset: str) -> tuple[int, str]:
    """POST /videos to ytdl-sub-api. Returns (exit_code, output_tail).

    Returns (-1, msg) if the upstream endpoint isn't deployed yet (404)
    so the UI can show a useful message.
    """
    try:
        r = httpx.post(
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

def _flat_listing(url: str, limit: int) -> list[dict]:
    """yt-dlp --flat-playlist --dump-json. Each line is one video JSON."""
    cmd = [
        "yt-dlp",
        "--flat-playlist",
        "--dump-json",
        "--ignore-errors",
        # `youtubetab:approximate_date` makes the channel-tab extractor
        # surface relative dates ("2 weeks ago") as a `timestamp` field
        # on flat-playlist entries. Without this, flat-playlist rows
        # come back with no upload_date at all and our cutoff window
        # can't filter them.
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


def poll_channel(db: sqlite3.Connection, name: str, url: str) -> tuple[int, str | None]:
    """Returns (new_video_count, error_message)."""
    now = int(time.time())
    try:
        entries = _flat_listing(url, POLL_LIMIT)
    except Exception as exc:  # noqa: BLE001
        db.execute(
            "UPDATE channels SET last_polled_at = ?, last_error = ? WHERE name = ?",
            (now, str(exc)[:500], name),
        )
        return 0, str(exc)
    new_count = 0
    for e in entries:
        vid = e.get("id")
        if not vid:
            continue
        # Members-only / Premium-only / login-walled videos can't be
        # downloaded by ytdl-sub anyway and would just clutter the
        # board. Skip at poll-time so they never enter the DB.
        if e.get("availability") in SKIP_AVAILABILITY:
            continue
        # Shorts: detected by /shorts/ in the entry URL. Skip at poll
        # time so they never enter the DB — same pattern as the
        # availability gate above.
        if _is_short(e):
            continue
        existing = db.execute(
            "SELECT status FROM videos WHERE video_id = ?", (vid,)
        ).fetchone()
        title = e.get("title")
        duration = int(e["duration"]) if isinstance(e.get("duration"), (int, float)) else None
        upload_date = e.get("upload_date")
        # Fallback: yt-dlp's youtubetab extractor (with approximate_date)
        # often returns `timestamp` instead of `upload_date`. Convert it
        # to the YYYYMMDD form we already use everywhere else.
        if not upload_date:
            ts = e.get("timestamp")
            if isinstance(ts, (int, float)) and ts > 0:
                from datetime import datetime, timezone
                upload_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y%m%d")
        thumb = _best_thumbnail(e)
        watch_url = e.get("url") or e.get("webpage_url") or f"https://www.youtube.com/watch?v={vid}"
        if existing is None:
            db.execute(
                """INSERT INTO videos
                   (video_id, channel_name, title, duration, upload_date,
                    thumbnail_url, url, status, seen_at, status_changed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'new', ?, ?)""",
                (vid, name, title, duration, upload_date, thumb, watch_url, now, now),
            )
            new_count += 1
        else:
            db.execute(
                """UPDATE videos
                      SET title = COALESCE(?, title),
                          duration = COALESCE(?, duration),
                          upload_date = COALESCE(?, upload_date),
                          thumbnail_url = COALESCE(?, thumbnail_url),
                          url = COALESCE(?, url)
                    WHERE video_id = ?""",
                (title, duration, upload_date, thumb, watch_url, vid),
            )
    db.execute(
        "UPDATE channels SET last_polled_at = ?, last_error = NULL WHERE name = ?",
        (now, name),
    )
    return new_count, None


def sync_channels_from_api(db: sqlite3.Connection) -> None:
    """Mirror ytdl-sub-api's channel list into our local table.

    Channels removed upstream are also removed locally; their videos
    cascade off-board (we filter on join).
    """
    remote = fetch_channels_from_api()
    seen = set()
    for c in remote:
        name = c.get("name")
        url = c.get("url")
        # ytdl-sub-api returns `preset` as a string for Shape-1 (chained-
        # preset block) channels, but as a list for Shape-2 (standalone
        # subscription with inline preset chain). SQLite rejects lists, so
        # the whole sync would fail mid-loop on the first standalone sub.
        # Coerce to a " | "-joined string — matches the chained-preset key
        # format used by Shape-1 entries.
        raw_preset = c.get("preset")
        if isinstance(raw_preset, list):
            preset = " | ".join(str(p) for p in raw_preset)
        else:
            preset = raw_preset or ""
        if not name or not url:
            continue
        seen.add(name)
        db.execute(
            """INSERT INTO channels (name, url, preset)
               VALUES (?, ?, ?)
               ON CONFLICT(name) DO UPDATE SET url = excluded.url, preset = excluded.preset""",
            (name, url, preset),
        )
    if seen:
        placeholders = ",".join("?" * len(seen))
        db.execute(
            f"DELETE FROM channels WHERE name NOT IN ({placeholders})",
            tuple(seen),
        )


def poll_all() -> dict:
    """Background task: sync channel list, then poll each channel."""
    started = time.time()
    summary = {"channels": 0, "new_videos": 0, "errors": []}
    try:
        with standalone_db() as db:
            try:
                sync_channels_from_api(db)
            except Exception as exc:  # noqa: BLE001
                summary["errors"].append(f"sync: {exc}")
            rows = db.execute("SELECT name, url FROM channels").fetchall()
            summary["channels"] = len(rows)
            for row in rows:
                count, err = poll_channel(db, row["name"], row["url"])
                summary["new_videos"] += count
                if err:
                    summary["errors"].append(f"{row['name']}: {err}")
    except Exception as exc:  # noqa: BLE001
        summary["errors"].append(f"fatal: {exc}")
    summary["elapsed_seconds"] = round(time.time() - started, 2)
    print(f"poll_all: {summary}", file=sys.stderr, flush=True)
    return summary


# --- Routes -----------------------------------------------------------

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.get("/login")
def login_form():
    return render_template("login.html", error=None)


@app.post("/login")
def login_submit():
    token = (request.form.get("token") or "").strip()
    if token != API_TOKEN:
        return render_template("login.html", error="invalid token"), 401
    resp = redirect("/")
    resp.set_cookie(
        "wytchr_token", token, httponly=True, samesite="Lax", max_age=60 * 60 * 24 * 30
    )
    return resp


@app.get("/")
@auth_required
def index():
    return render_template("board.html", poll_interval=POLL_INTERVAL_MINUTES)


@app.get("/board")
@auth_required
def board_partial():
    db = get_db()
    channels = db.execute(
        "SELECT name, url, preset, last_polled_at, last_error FROM channels ORDER BY name COLLATE NOCASE"
    ).fetchall()
    show_hidden = request.args.get("hidden") == "1"
    # Tag filter — `?tag=foo` narrows the board to channels that either
    # carry the tag (then show all their videos) or have at least one
    # video with the tag (then show just those). Channels matching
    # neither are hidden entirely.
    tag_filter_name = _normalize_tag(request.args.get("tag", ""))
    tag_filter_id: int | None = None
    if tag_filter_name:
        row = db.execute(
            "SELECT id FROM tags WHERE name = ?", (tag_filter_name,)
        ).fetchone()
        # Sentinel -1 = the requested tag exists in the URL but not in
        # the DB → match nothing rather than fall through to no-filter.
        tag_filter_id = row["id"] if row else -1

    if show_hidden:
        status_filter = ("hidden",)
    else:
        # wytchr is a *selector*, not a library view: only show videos
        # the operator hasn't acted on yet. `done` and `hidden` are
        # already-decided and would just be clutter — their counts
        # show in the per-channel summary and the global stats bar.
        status_filter = ("new", "queued", "downloading", "failed")
    status_placeholders = ",".join("?" * len(status_filter))

    # Profile-driven display window: each channel's preset chain points
    # at a profile (daily/long_collection/...) whose only_recent_date_range
    # and only_recent_max_files become wytchr's "show videos newer than
    # X / cap at Y" rule. Same fields ytdl-sub uses for download
    # retention — single source of truth for "what's recent."
    presets_data = fetch_presets()
    profile_details = presets_data.get("profile_details") or {}
    settings_map = _channel_settings_map(db)
    from datetime import datetime, timedelta, timezone
    now_dt = datetime.now(timezone.utc)
    now_ts = int(now_dt.timestamp())

    columns = []
    for ch in channels:
        cs = settings_map.get(ch["name"], _CHANNEL_SETTINGS_DEFAULTS)
        # hide_channel: drop from the board entirely. show_hidden bypass
        # so the operator can find a hidden channel to un-hide it.
        if cs["hide_channel"] and not show_hidden:
            continue
        # Window resolution: per-channel override wins; profile is the
        # fallback. days/max_files independently overridable.
        prof_days, prof_max = _channel_window(ch["preset"], profile_details)
        cs_days = _parse_date_range(cs["date_range"]) if cs["date_range"] else None
        days = cs_days if cs_days is not None else prof_days
        cs_max = cs["max_files"]
        max_files = cs_max if (isinstance(cs_max, int) and cs_max > 0) else prof_max

        # Auto-mark watched: any 'new'/'failed' row whose upload_date is
        # older than the channel's auto_watched_days flips to 'watched'
        # before counting/listing. Cheap UPDATE, no-op when unset.
        aw = cs["auto_watched_days"]
        if isinstance(aw, int) and aw > 0:
            aw_cutoff = (now_dt - timedelta(days=aw)).strftime("%Y%m%d")
            db.execute(
                """UPDATE videos
                      SET status = 'watched', status_changed_at = ?
                    WHERE channel_name = ?
                      AND status IN ('new', 'failed')
                      AND upload_date IS NOT NULL
                      AND upload_date < ?""",
                (now_ts, ch["name"], aw_cutoff),
            )

        # YYYYMMDD lex compare matches yt-dlp's flat-playlist format.
        cutoff = None
        if days is not None:
            cutoff = (now_dt - timedelta(days=days)).strftime("%Y%m%d")
        per_channel_limit = max_files if (max_files is not None and max_files > 0) else 20

        shorts_clause = "" if cs["include_shorts"] else " AND v.url NOT LIKE '%/shorts/%'"
        shorts_clause_count = "" if cs["include_shorts"] else " AND url NOT LIKE '%/shorts/%'"
        title_inc_re = _compile_title_re(cs["title_include"])
        title_exc_re = _compile_title_re(cs["title_exclude"])

        # Counts: per-status histogram WITHIN the display window so the
        # summary "5 new" matches what the operator actually sees on the
        # board. Old videos drop out of view AND out of the counts.
        # `show_hidden` mode disables the window — operator's auditing
        # past dismissals.
        if cutoff and not show_hidden:
            counts = {
                row["status"]: row["n"]
                for row in db.execute(
                    f"""SELECT status, COUNT(*) AS n FROM videos
                         WHERE channel_name = ?
                           {shorts_clause_count}
                           AND upload_date IS NOT NULL
                           AND upload_date >= ?
                      GROUP BY status""",
                    (ch["name"], cutoff),
                ).fetchall()
            }
        else:
            counts = {
                row["status"]: row["n"]
                for row in db.execute(
                    f"SELECT status, COUNT(*) AS n FROM videos WHERE channel_name = ? {shorts_clause_count} GROUP BY status",
                    (ch["name"],),
                ).fetchall()
            }
        actionable = counts.get("new", 0) + counts.get("failed", 0)

        # Display window predicate: only fold into the SELECT when the
        # profile actually set a date_range AND we're not in show_hidden
        # mode. show_hidden bypasses the window so the operator can
        # audit older dismissals.
        window_clause = ""
        window_args: tuple = ()
        if cutoff and not show_hidden:
            # Strict: rows with NULL upload_date are excluded. A
            # seen_at fallback was tried but lets months-old videos
            # ride a recent poll's seen_at through the window. The
            # youtubetab:approximate_date extractor-arg now populates
            # upload_date for new polls; legacy NULL rows just won't
            # show until they're refreshed (or aged out via cleanup).
            window_clause = " AND v.upload_date IS NOT NULL AND v.upload_date >= ?"
            window_args = (cutoff,)

        if tag_filter_id is not None:
            channel_has_tag = db.execute(
                "SELECT 1 FROM channel_tags WHERE channel_name = ? AND tag_id = ?",
                (ch["name"], tag_filter_id),
            ).fetchone()
            if channel_has_tag:
                videos = db.execute(
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
                videos = db.execute(
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
                    # No channel-tag and no matching videos — skip the
                    # whole column. This is the only place we drop a
                    # channel from the board entirely.
                    continue
        else:
            videos = db.execute(
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

        # Title-regex post-filter. Done in Python so the operator can
        # paste any Python regex; SQLite's LIKE is too coarse and
        # binding REGEXP would mean shipping a sqlite3 extension.
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
    # Persist any auto-mark-watched flips done in the channel loop.
    db.commit()

    # Group columns by their profile so the board renders one section
    # per profile + a trailing "ungrouped" bucket for Shape-2 / no-
    # profile subscriptions. Section order tracks profile_details
    # insertion order from /presets, with the empty-key bucket last.
    sections: list[dict] = []
    section_idx: dict[str, int] = {}
    for profile_name in profile_details:
        section_idx[profile_name] = len(sections)
        sections.append({"profile": profile_name, "columns": []})
    # ungrouped bucket: created on demand
    for col in columns:
        profile_name = _resolve_profile(col["channel"]["preset"], profile_details)
        if profile_name and profile_name in section_idx:
            sections[section_idx[profile_name]]["columns"].append(col)
        else:
            if "" not in section_idx:
                section_idx[""] = len(sections)
                sections.append({"profile": "", "columns": []})
            sections[section_idx[""]]["columns"].append(col)
    # Within each section, sink channels with no visible videos to
    # the bottom — actionable rows float up, "caught up" / empty rows
    # collect at the end. Stable sort preserves the API's channel
    # order within each bucket.
    for section in sections:
        section["columns"].sort(key=lambda c: 0 if c["videos"] else 1)
    # Drop empty sections from a tag-filtered view, but always keep
    # them visible when no filter is active so they can be drop
    # targets for empty profiles.
    if tag_filter_name:
        sections = [s for s in sections if s["columns"]]

    # Tag maps for inline chip rendering on cards + summaries. One
    # query each, joined to tag names.
    all_video_ids = [v["video_id"] for col in columns for v in col["videos"]]
    video_tags_map = _video_tags_map(db, all_video_ids)
    channel_tags_map = _channel_tags_map(db, [c["channel"]["name"] for c in columns])

    # All tags + their use-counts for the filter chip bar.
    all_tags = db.execute(
        """SELECT t.name,
                  (SELECT COUNT(*) FROM video_tags WHERE tag_id = t.id) AS video_count,
                  (SELECT COUNT(*) FROM channel_tags WHERE tag_id = t.id) AS channel_count
             FROM tags t
         ORDER BY t.name COLLATE NOCASE"""
    ).fetchall()

    # Group by `prefix:suffix` convention. Plain tags land under "".
    # Insertion order = alphabetic from the SELECT above, so iterating
    # the dict yields a stable, sorted display order.
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
        "channels": len(channels),  # global count, not filtered count
        "videos": sum(sum(c["counts"].values()) for c in columns),
        "new": sum(c["counts"].get("new", 0) for c in columns),
        "failed": sum(c["counts"].get("failed", 0) for c in columns),
        "queued": sum(c["counts"].get("queued", 0) + c["counts"].get("downloading", 0) for c in columns),
        "done": sum(c["counts"].get("done", 0) for c in columns),
        "watched": sum(c["counts"].get("watched", 0) for c in columns),
    }
    return render_template(
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


def _name_from_request() -> str:
    if request.is_json:
        return ((request.get_json(silent=True) or {}).get("name") or "").strip()
    return (request.form.get("name") or "").strip()


def _render_video_tags(db: sqlite3.Connection, video_id: str):
    tags = [
        r["name"]
        for r in db.execute(
            """SELECT t.name FROM video_tags vt
                 JOIN tags t ON t.id = vt.tag_id
                WHERE vt.video_id = ?
             ORDER BY t.name COLLATE NOCASE""",
            (video_id,),
        ).fetchall()
    ]
    return render_template("_tags_video.html", video_id=video_id, tags=tags)


def _render_channel_tags(db: sqlite3.Connection, channel_name: str):
    tags = [
        r["name"]
        for r in db.execute(
            """SELECT t.name FROM channel_tags ct
                 JOIN tags t ON t.id = ct.tag_id
                WHERE ct.channel_name = ?
             ORDER BY t.name COLLATE NOCASE""",
            (channel_name,),
        ).fetchall()
    ]
    return render_template("_tags_channel.html", channel_name=channel_name, tags=tags)


@app.post("/videos/<video_id>/tags")
@auth_required
def add_video_tag(video_id: str):
    db = get_db()
    if not db.execute("SELECT 1 FROM videos WHERE video_id = ?", (video_id,)).fetchone():
        return ("video not found", 404)
    tag_id = _upsert_tag(db, _name_from_request())
    if tag_id is None:
        return _render_video_tags(db, video_id)
    db.execute(
        "INSERT OR IGNORE INTO video_tags (video_id, tag_id) VALUES (?, ?)",
        (video_id, tag_id),
    )
    db.commit()
    return _render_video_tags(db, video_id)


@app.delete("/videos/<video_id>/tags/<tag_name>")
@auth_required
def delete_video_tag(video_id: str, tag_name: str):
    db = get_db()
    norm = _normalize_tag(tag_name)
    db.execute(
        """DELETE FROM video_tags
                 WHERE video_id = ?
                   AND tag_id = (SELECT id FROM tags WHERE name = ?)""",
        (video_id, norm),
    )
    db.commit()
    return _render_video_tags(db, video_id)


@app.post("/channels/<channel_name>/tags")
@auth_required
def add_channel_tag(channel_name: str):
    db = get_db()
    if not db.execute(
        "SELECT 1 FROM channels WHERE name = ?", (channel_name,)
    ).fetchone():
        return ("channel not found", 404)
    tag_id = _upsert_tag(db, _name_from_request())
    if tag_id is None:
        return _render_channel_tags(db, channel_name)
    db.execute(
        "INSERT OR IGNORE INTO channel_tags (channel_name, tag_id) VALUES (?, ?)",
        (channel_name, tag_id),
    )
    db.commit()
    return _render_channel_tags(db, channel_name)


@app.delete("/channels/<channel_name>/tags/<tag_name>")
@auth_required
def delete_channel_tag(channel_name: str, tag_name: str):
    db = get_db()
    norm = _normalize_tag(tag_name)
    db.execute(
        """DELETE FROM channel_tags
                 WHERE channel_name = ?
                   AND tag_id = (SELECT id FROM tags WHERE name = ?)""",
        (channel_name, norm),
    )
    db.commit()
    return _render_channel_tags(db, channel_name)


@app.get("/tags")
@auth_required
def tags_admin():
    db = get_db()
    rows = db.execute(
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
    return render_template("tags.html", grouped=grouped, flash=flash)


@app.post("/channels/<channel_name>/profile")
@auth_required
def channel_change_profile(channel_name: str):
    """Reassign a channel's profile (DELETE + POST upstream).

    Body: {profile: "long_collection"} — empty string / missing
    profile drops the channel back to the base preset (no chain).
    Failure mode: if DELETE succeeds but POST fails, the channel is
    briefly unsubscribed; we surface the error so the operator can
    retry from the same UI without losing the URL.
    """
    body = request.get_json(silent=True) or {}
    new_profile = (body.get("profile") or "").strip()
    db = get_db()
    row = db.execute(
        "SELECT url FROM channels WHERE name = ?", (channel_name,)
    ).fetchone()
    if not row:
        return jsonify({"error": "channel not found in wytchr DB"}), 404
    url = row["url"]

    # DELETE — 404 is fine (e.g. operator already removed it elsewhere
    # and we're catching up). 200 is success. Anything else aborts so
    # we don't double-up subscriptions.
    try:
        d = httpx.delete(
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
        p = httpx.post(
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

    # Mirror the new preset into wytchr's channels table so the next
    # /board render lands the channel in the right section without
    # waiting for sync_channels_from_api.
    presets_data = fetch_presets(force=True)
    base = presets_data.get("base_preset", "")
    new_preset = f"{base} | {new_profile}" if new_profile else base
    db.execute(
        "UPDATE channels SET preset = ? WHERE name = ?",
        (new_preset, channel_name),
    )
    db.commit()
    return jsonify({"ok": True, "preset": new_preset})


# --- Per-channel settings ---------------------------------------------

def _get_channel_settings_row(db: sqlite3.Connection, channel_name: str) -> dict:
    """Return current settings (or defaults) for one channel. Always
    returns a dict — call sites can `.get()` without a None check."""
    r = db.execute(
        """SELECT display_name, date_range, max_files,
                  include_shorts, hide_channel, auto_watched_days,
                  title_include, title_exclude
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
    }


@app.get("/channels/<channel_name>/settings")
@auth_required
def channel_settings_page(channel_name: str):
    db = get_db()
    ch = db.execute(
        "SELECT name, url, preset FROM channels WHERE name = ?", (channel_name,)
    ).fetchone()
    if not ch:
        return ("channel not found", 404)
    settings = _get_channel_settings_row(db, channel_name)
    presets_data = fetch_presets()
    profile_details = presets_data.get("profile_details") or {}
    prof_days, prof_max = _channel_window(ch["preset"], profile_details)
    return render_template(
        "channel_settings.html",
        channel=ch,
        settings=settings,
        profile_days=prof_days,
        profile_max=prof_max,
        saved=request.args.get("saved") == "1",
    )


@app.post("/channels/<channel_name>/settings")
@auth_required
def channel_settings_save(channel_name: str):
    db = get_db()
    if not db.execute("SELECT 1 FROM channels WHERE name = ?", (channel_name,)).fetchone():
        return ("channel not found", 404)
    f = request.form

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
    }
    # Validate regex patterns up front so a bad pattern fails loudly
    # instead of silently being a no-op at render time.
    for key in ("title_include", "title_exclude"):
        if payload[key] and _compile_title_re(payload[key]) is None:
            return (f"invalid regex for {key}: {payload[key]}", 400)
    # Validate date_range against the same parser used for the window.
    if payload["date_range"] and _parse_date_range(payload["date_range"]) is None:
        return (f"invalid date_range (try '7days', '2weeks', '6months', '1year')", 400)

    db.execute(
        """INSERT INTO channel_settings
             (channel_name, display_name, date_range, max_files,
              include_shorts, hide_channel, auto_watched_days,
              title_include, title_exclude, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(channel_name) DO UPDATE SET
             display_name = excluded.display_name,
             date_range = excluded.date_range,
             max_files = excluded.max_files,
             include_shorts = excluded.include_shorts,
             hide_channel = excluded.hide_channel,
             auto_watched_days = excluded.auto_watched_days,
             title_include = excluded.title_include,
             title_exclude = excluded.title_exclude,
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
            int(time.time()),
        ),
    )
    db.commit()
    return redirect(f"/channels/{channel_name}/settings?saved=1")


# --- Presets admin (proxy to ytdl-sub-api) ----------------------------

def _split_csv(s: str) -> list[str]:
    return [p.strip() for p in (s or "").split(",") if p.strip()]


@app.get("/presets")
@auth_required
def presets_admin():
    data = fetch_presets(force=True)
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
            # Other override keys, rendered as a textarea so the user
            # can keep arbitrary ytdl-sub overrides without us
            # second-guessing the schema.
            "extra": {k: v for k, v in overrides.items() if k not in ("only_recent_date_range", "only_recent_max_files")},
        })
    flash = request.args.get("flash") or ""
    flash_kind = request.args.get("kind") or "ok"
    return render_template(
        "presets.html",
        profiles=profiles,
        base_preset=base_preset,
        flash=flash,
        flash_kind=flash_kind,
    )


def _overrides_from_form() -> dict:
    """Reconstruct an overrides dict from the admin form: the two
    visible fields plus a freeform `extra` textarea (`key=value` per
    line). Empty values are dropped so the upstream PATCH can clear a
    field by sending an empty `overrides`."""
    out: dict = {}
    date_range = (request.form.get("date_range") or "").strip()
    if date_range:
        out["only_recent_date_range"] = date_range
    max_files_raw = (request.form.get("max_files") or "").strip()
    if max_files_raw:
        try:
            out["only_recent_max_files"] = int(max_files_raw)
        except ValueError:
            pass
    extra = request.form.get("extra") or ""
    for line in extra.splitlines():
        line = line.strip()
        if not line or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip()
        if not k:
            continue
        # Try to coerce numeric, otherwise leave as string.
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
def presets_create():
    name = (request.form.get("name") or "").strip()
    parents = _split_csv(request.form.get("parents") or "")
    overrides = _overrides_from_form()
    payload = {"name": name, "parents": parents, "overrides": overrides}
    try:
        r = httpx.post(
            f"{YTDL_SUB_API_URL}/presets",
            headers=_api_headers(),
            json=payload,
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    fetch_presets(force=True)  # bust cache so the page reflects truth
    if r.status_code == 201:
        return redirect(f"/presets?flash=created+{name}")
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return redirect(f"/presets?kind=err&flash={body.get('error', 'create+failed')}")


@app.post("/presets/<name>/update")
@auth_required
def presets_update(name: str):
    parents = _split_csv(request.form.get("parents") or "")
    overrides = _overrides_from_form()
    payload = {"parents": parents, "overrides": overrides}
    try:
        r = httpx.patch(
            f"{YTDL_SUB_API_URL}/presets/{name}",
            headers=_api_headers(),
            json=payload,
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    fetch_presets(force=True)
    if r.status_code == 200:
        return redirect(f"/presets?flash=updated+{name}")
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    return redirect(f"/presets?kind=err&flash={body.get('error', 'update+failed')}")


@app.post("/presets/<name>/delete")
@auth_required
def presets_delete(name: str):
    try:
        r = httpx.delete(
            f"{YTDL_SUB_API_URL}/presets/{name}",
            headers=_api_headers(),
            timeout=15,
        )
    except Exception as exc:  # noqa: BLE001
        return redirect(f"/presets?kind=err&flash=upstream+unreachable:+{exc}")
    fetch_presets(force=True)
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
def create_tag():
    raw = (request.form.get("name") or "").strip()
    norm = _normalize_tag(raw)
    if not norm:
        return redirect("/tags?flash=invalid+name")
    db = get_db()
    existing = db.execute("SELECT 1 FROM tags WHERE name = ?", (norm,)).fetchone()
    if existing:
        return redirect(f"/tags?flash=tag+{norm}+already+exists")
    _upsert_tag(db, norm)
    db.commit()
    return redirect(f"/tags?flash=created+{norm}")


@app.post("/tags/<old_name>/rename")
@auth_required
def rename_tag(old_name: str):
    new_raw = (request.form.get("new_name") or "").strip()
    new_norm = _normalize_tag(new_raw)
    if not new_norm:
        return redirect(f"/tags?flash=invalid+name")
    db = get_db()
    old_row = db.execute("SELECT id FROM tags WHERE name = ?", (old_name,)).fetchone()
    if not old_row:
        return redirect(f"/tags?flash=tag+not+found")
    if old_name == new_norm:
        return redirect("/tags")
    old_id = old_row["id"]
    existing = db.execute(
        "SELECT id FROM tags WHERE name = ?", (new_norm,)
    ).fetchone()
    if existing and existing["id"] != old_id:
        # Merge: repoint joins from old → existing, drop the old tag.
        # INSERT OR IGNORE skips rows that would duplicate the new
        # composite key; the subsequent DELETE clears the old half.
        keep_id = existing["id"]
        db.execute(
            "INSERT OR IGNORE INTO video_tags (video_id, tag_id) "
            "SELECT video_id, ? FROM video_tags WHERE tag_id = ?",
            (keep_id, old_id),
        )
        db.execute("DELETE FROM video_tags WHERE tag_id = ?", (old_id,))
        db.execute(
            "INSERT OR IGNORE INTO channel_tags (channel_name, tag_id) "
            "SELECT channel_name, ? FROM channel_tags WHERE tag_id = ?",
            (keep_id, old_id),
        )
        db.execute("DELETE FROM channel_tags WHERE tag_id = ?", (old_id,))
        db.execute("DELETE FROM tags WHERE id = ?", (old_id,))
        db.commit()
        return redirect(f"/tags?flash=merged+into+{new_norm}")
    db.execute("UPDATE tags SET name = ? WHERE id = ?", (new_norm, old_id))
    db.commit()
    return redirect(f"/tags?flash=renamed+to+{new_norm}")


@app.post("/tags/<name>/delete")
@auth_required
def delete_tag(name: str):
    db = get_db()
    db.execute("DELETE FROM tags WHERE name = ?", (name,))
    db.commit()
    return redirect(f"/tags?flash=deleted+{name}")


@app.post("/poll/all")
@auth_required
def poll_all_route():
    summary = poll_all()
    if request.headers.get("HX-Request"):
        resp = board_partial()
        # Surface the run summary client-side via HX-Trigger; the
        # board.html toast handler picks `wytchr:polled` up and shows a
        # short banner with channels / new / errors / elapsed.
        if isinstance(resp, str):
            from flask import make_response
            resp = make_response(resp)
        resp.headers["HX-Trigger"] = json.dumps({"wytchr:polled": summary})
        return resp
    return jsonify(summary)


@app.post("/videos/<video_id>/download")
@auth_required
def download_video(video_id: str):
    db = get_db()
    row = db.execute(
        """SELECT v.url, v.channel_name, c.preset
             FROM videos v
             JOIN channels c ON c.name = v.channel_name
            WHERE v.video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    now = int(time.time())
    db.execute(
        "UPDATE videos SET status = 'queued', status_changed_at = ? WHERE video_id = ?",
        (now, video_id),
    )
    db.commit()
    exit_code, output = request_download(row["url"], row["preset"])
    final_status = "done" if exit_code == 0 else "failed"
    db.execute(
        "UPDATE videos SET status = ?, last_output = ?, status_changed_at = ? WHERE video_id = ?",
        (final_status, output, int(time.time()), video_id),
    )
    db.commit()
    if request.headers.get("HX-Request"):
        return _render_card(video_id)
    return jsonify({"status": final_status, "exit_code": exit_code, "output": output})


@app.post("/videos/<video_id>/hide")
@auth_required
def hide_video(video_id: str):
    db = get_db()
    db.execute(
        "UPDATE videos SET status = 'hidden', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    db.commit()
    if request.headers.get("HX-Request"):
        return ("", 200)
    return jsonify({"ok": True})


@app.post("/videos/<video_id>/watched")
@auth_required
def watched_video(video_id: str):
    db = get_db()
    db.execute(
        "UPDATE videos SET status = 'watched', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    db.commit()
    if request.headers.get("HX-Request"):
        return ("", 200)
    return jsonify({"ok": True})


@app.post("/videos/<video_id>/unhide")
@auth_required
def unhide_video(video_id: str):
    db = get_db()
    db.execute(
        "UPDATE videos SET status = 'new', status_changed_at = ? WHERE video_id = ?",
        (int(time.time()), video_id),
    )
    db.commit()
    if request.headers.get("HX-Request"):
        return _render_card(video_id)
    return jsonify({"ok": True})


def _render_card(video_id: str):
    db = get_db()
    row = db.execute(
        """SELECT video_id, title, duration, upload_date, thumbnail_url, url, status, channel_name, favorited_at, description
             FROM videos WHERE video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return ("", 404)
    return render_template("_card.html", v=row)


# --- Favorites + webhooks --------------------------------------------

def _video_payload(row: sqlite3.Row) -> dict:
    # Receivers (e.g. all-my-favs) typically expect bookmark fields
    # at the top level: url, title, notes. We map description→notes.
    desc = None
    try:
        desc = row["description"]
    except (IndexError, KeyError):
        pass
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


def _enrich_and_fire_async(video_id: str) -> None:
    """Bg: backfill description (if missing), then fire the favorite
    webhook. Caller has already toggled favorited_at on."""
    def run():
        with standalone_db() as db:
            row = db.execute(
                """SELECT video_id, title, duration, upload_date, thumbnail_url,
                          url, status, channel_name, favorited_at, description
                     FROM videos WHERE video_id = ?""",
                (video_id,),
            ).fetchone()
            if not row:
                return
            if not row["description"] and row["url"]:
                desc = _fetch_description(row["url"])
                if desc:
                    db.execute(
                        "UPDATE videos SET description = ? WHERE video_id = ?",
                        (desc, video_id),
                    )
                    db.commit()
                    row = db.execute(
                        """SELECT video_id, title, duration, upload_date, thumbnail_url,
                                  url, status, channel_name, favorited_at, description
                             FROM videos WHERE video_id = ?""",
                        (video_id,),
                    ).fetchone()
        _fire_webhooks_sync("video.favorited", _video_payload(row))
    threading.Thread(target=run, daemon=True).start()


def _fetch_description(video_url: str) -> str | None:
    """Pull a video's description via the YouTube Data API v3. Returns
    None if no API key is set, the URL doesn't carry an 11-char id, or
    the call fails — caller treats None as "skip" and stores NULL.

    yt-dlp can't do this from the K6 IP without auth (bot-gate), and
    the operator has rejected cookies. The Data API has a 10k-unit
    daily free quota; videos.list?part=snippet is 1 unit per call."""
    if not YOUTUBE_API_KEY:
        return None
    m = _VIDEO_ID_RE.search(video_url)
    if not m:
        return None
    try:
        r = httpx.get(
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


def _fire_webhooks_sync(event: str, payload: dict, hook_id: int | None = None) -> None:
    """POST to webhooks synchronously. Caller is responsible for
    running this off the request thread when latency matters."""
    with standalone_db() as db:
        if hook_id is not None:
            hooks = db.execute(
                "SELECT id, url, bearer_token FROM webhooks WHERE id = ?",
                (hook_id,),
            ).fetchall()
        else:
            hooks = db.execute(
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
            r = httpx.post(h["url"], json=body, headers=headers, timeout=10.0)
            status = r.status_code
            if r.status_code >= 400:
                err = (r.text or "")[:500]
        except Exception as e:
            err = str(e)[:500]
        with standalone_db() as db:
            db.execute(
                "UPDATE webhooks SET last_fired_at = ?, last_status = ?, last_error = ? WHERE id = ?",
                (int(time.time()), status, err, h["id"]),
            )


def _fire_webhooks_async(event: str, payload: dict, hook_id: int | None = None) -> None:
    """Fire-and-forget wrapper around _fire_webhooks_sync."""
    threading.Thread(
        target=_fire_webhooks_sync, args=(event, payload, hook_id), daemon=True,
    ).start()


@app.post("/videos/<video_id>/favorite")
@auth_required
def favorite_video(video_id: str):
    db = get_db()
    row = db.execute(
        """SELECT v.video_id, v.title, v.duration, v.upload_date, v.thumbnail_url,
                  v.url, v.status, v.channel_name, v.favorited_at, v.description
             FROM videos v WHERE v.video_id = ?""",
        (video_id,),
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    now = int(time.time())
    if row["favorited_at"]:
        db.execute("UPDATE videos SET favorited_at = NULL WHERE video_id = ?", (video_id,))
        db.commit()
        favorited = False
    else:
        db.execute("UPDATE videos SET favorited_at = ? WHERE video_id = ?", (now, video_id))
        db.commit()
        favorited = True
        # Backfill description (1 YouTube Data API unit) and fire the
        # webhook in the same background thread so the UI returns
        # immediately. Description has to land before the POST or the
        # receiver gets notes=null.
        _enrich_and_fire_async(video_id)
    if request.headers.get("HX-Request"):
        return _render_card(video_id)
    return jsonify({"ok": True, "favorited": favorited})


@app.post("/videos/<video_id>/fetch-description")
@auth_required
def fetch_video_description(video_id: str):
    """On-demand description fetch. Synchronous so the operator gets
    the updated card back in the same response."""
    db = get_db()
    row = db.execute(
        "SELECT video_id, url FROM videos WHERE video_id = ?", (video_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "video not found"}), 404
    desc = _fetch_description(row["url"]) if row["url"] else None
    if desc:
        db.execute(
            "UPDATE videos SET description = ? WHERE video_id = ?",
            (desc, video_id),
        )
        db.commit()
    if request.headers.get("HX-Request"):
        return _render_card(video_id)
    return jsonify({"ok": bool(desc), "description": desc})


@app.get("/favorites")
@auth_required
def favorites_page():
    db = get_db()
    videos = db.execute(
        """SELECT video_id, title, duration, upload_date, thumbnail_url, url,
                  status, channel_name, favorited_at
             FROM videos
            WHERE favorited_at IS NOT NULL
         ORDER BY favorited_at DESC""",
    ).fetchall()
    video_tags_map = _video_tags_map(db, [v["video_id"] for v in videos])
    return render_template(
        "favorites.html",
        videos=videos,
        video_tags_map=video_tags_map,
    )


@app.get("/webhooks")
@auth_required
def webhooks_admin():
    db = get_db()
    hooks = db.execute(
        "SELECT id, name, url, event, enabled, bearer_token, created_at, last_fired_at, last_status, last_error FROM webhooks ORDER BY id"
    ).fetchall()
    return render_template("webhooks.html", hooks=hooks)


@app.post("/webhooks")
@auth_required
def webhooks_create():
    name = (request.form.get("name") or "").strip()
    url = (request.form.get("url") or "").strip()
    event = (request.form.get("event") or "video.favorited").strip()
    bearer = (request.form.get("bearer_token") or "").strip() or None
    if not name or not url:
        return ("name and url are required", 400)
    if not (url.startswith("http://") or url.startswith("https://")):
        return ("url must be http(s)", 400)
    db = get_db()
    db.execute(
        "INSERT INTO webhooks (name, url, event, bearer_token, created_at) VALUES (?, ?, ?, ?, ?)",
        (name, url, event, bearer, int(time.time())),
    )
    db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/toggle")
@auth_required
def webhooks_toggle(hook_id: int):
    db = get_db()
    db.execute("UPDATE webhooks SET enabled = 1 - enabled WHERE id = ?", (hook_id,))
    db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/delete")
@auth_required
def webhooks_delete(hook_id: int):
    db = get_db()
    db.execute("DELETE FROM webhooks WHERE id = ?", (hook_id,))
    db.commit()
    return redirect("/webhooks")


@app.post("/webhooks/<int:hook_id>/test")
@auth_required
def webhooks_test(hook_id: int):
    db = get_db()
    row = db.execute(
        "SELECT event FROM webhooks WHERE id = ?", (hook_id,)
    ).fetchone()
    if not row:
        return ("not found", 404)
    _fire_webhooks_async(
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
    )
    return redirect("/webhooks")


# --- Bootstrap --------------------------------------------------------

def start_scheduler():
    sched = BackgroundScheduler(timezone="UTC")
    sched.add_job(poll_all, "interval", minutes=POLL_INTERVAL_MINUTES, next_run_time=None)
    sched.start()
    return sched


init_db()
_scheduler = start_scheduler()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("FLASK_RUN_PORT", 5000)))
