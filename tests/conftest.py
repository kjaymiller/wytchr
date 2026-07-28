"""Functional-test harness.

These tests drive the real Quart app against a real Postgres — the SQL
shim in app.py rewrites SQLite-flavoured SQL on the way out, so an
in-memory fake would test the wrong thing entirely. Every regression
covered here was found by hand during review; the point of the suite is
that the next one gets found by CI instead.

Point DATABASE_URL at a throwaway PG (the pitchfork dev instance on
:5433 is the default). The whole suite skips, loudly, if nothing is
listening — a silent pass on an unreachable DB is worse than no suite.
"""

import os
import pathlib
import sys
import time

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Must precede `import app` — app.py reads these at module scope and
# sys.exit(1)s on anything missing.
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("YOUTUBE_API_KEY", "AIza" + "0" * 35)
os.environ.setdefault(
    "DATABASE_URL", "postgres://wytchr:wytchr@127.0.0.1:5433/wytchr"
)
# Harmless on revisions that predate the Valkey cache; required after it.
os.environ.setdefault("VALKEY_URL", "redis://127.0.0.1:6379/0")

import app as wytchr  # noqa: E402

try:
    import valkey.asyncio as valkey_asyncio  # noqa: E402
except ImportError:  # pragma: no cover — pre-cache revisions
    valkey_asyncio = None

TOKEN = os.environ["API_TOKEN"]
AUTH = {"Authorization": f"Bearer {TOKEN}"}

# Every table the app owns, in an order that satisfies the FKs. TRUNCATE
# ... CASCADE would do it in one line, but naming them keeps a new table
# from silently escaping the reset.
_TABLES = (
    "video_tags",
    "channel_tags",
    "tags",
    "videos",
    "channel_settings",
    "channels",
    "webhooks",
    "app_settings",
)


@pytest.fixture(scope="session", autouse=True)
def _require_db():
    """Sync on purpose: a session-scoped *async* fixture would have to
    share an event loop with the function-scoped tests, which
    pytest-asyncio rightly refuses. asyncio.run() sidesteps the whole
    question — the schema only needs creating once, before anything runs.
    """
    import asyncio

    try:
        asyncio.run(wytchr.init_db())
    except Exception as exc:  # noqa: BLE001 — any connect failure is a skip
        pytest.skip(
            f"no Postgres at {wytchr.DATABASE_URL!r} ({exc}); "
            "start one with `pitchfork start postgres` or set DATABASE_URL",
            allow_module_level=True,
        )


async def _reset_cache():
    """Drop every cached board slice between tests.

    Without this the suite lies to itself: TRUNCATE resets Postgres but
    leaves Valkey holding the previous test's rendered board, and since
    tests reuse channel names ("chan"), test N reads test N-1's cards.

    Deliberately *not* app._invalidate_all() — a fixture that cleans up
    by calling the code under test would hide a bug in that code. A wrong
    prefix there would leak state between tests and silence the test that
    should have caught it. So: our own scan, over the prefix constant.

    Scan-and-delete rather than FLUSHDB because VALKEY_URL may well point
    at a shared dev instance; only keys this app owns are ours to drop.
    """
    prefix = getattr(wytchr, "CACHE_PREFIX", None)
    if valkey_asyncio is None or prefix is None or not os.environ.get("VALKEY_URL"):
        return
    client = valkey_asyncio.Valkey.from_url(os.environ["VALKEY_URL"])
    try:
        keys = [
            k async for k in client.scan_iter(match=f"{prefix}:*", count=500)
        ]
        if keys:
            await client.unlink(*keys)
    finally:
        await client.aclose()


@pytest.fixture
async def client():
    """Authed test client, with before_serving/after_serving run.

    test_app() rather than test_client() because the app's lifecycle
    hooks build things the request path assumes exist (the HTTP client,
    and the cache handle once that lands).
    """
    async with wytchr.app.test_app() as test_app:
        async with wytchr.standalone_db() as db:
            for table in _TABLES:
                await db.execute(f"TRUNCATE {table} CASCADE")
        await _reset_cache()
        c = test_app.test_client()
        c.wytchr_auth = AUTH
        yield c


async def add_channel(name, *, preset="", url=None, polled=True):
    url = url or f"https://www.youtube.com/channel/UC{name:_<22.22}"
    async with wytchr.standalone_db() as db:
        await db.execute(
            "INSERT INTO channels (name, url, preset, last_polled_at) "
            "VALUES (?, ?, ?, ?)",
            (name, url, preset, int(time.time()) if polled else None),
        )


async def add_video(
    video_id,
    channel,
    *,
    title="a video",
    status="new",
    short=False,
    seen_at=None,
    watched_at=None,
):
    now = int(time.time())
    seen = now if seen_at is None else seen_at
    url = (
        f"https://www.youtube.com/shorts/{video_id}"
        if short
        else f"https://www.youtube.com/watch?v={video_id}"
    )
    async with wytchr.standalone_db() as db:
        await db.execute(
            """INSERT INTO videos (video_id, channel_name, title, duration,
                                   upload_date, thumbnail_url, url, status,
                                   seen_at, status_changed_at, watched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (video_id, channel, title, 120, "20260101", None, url, status,
             seen, now, watched_at),
        )


async def set_settings(channel, **kw):
    """Upsert a channel_settings row. Unset keys take the table default."""
    now = int(time.time())
    cols = {
        "display_name": None,
        "include_shorts": 0,
        "hide_channel": 0,
        "auto_watched_days": None,
        "title_include": None,
        "title_exclude": None,
    }
    cols.update(kw)
    async with wytchr.standalone_db() as db:
        await db.execute(
            """INSERT INTO channel_settings
                 (channel_name, display_name, include_shorts, hide_channel,
                  auto_watched_days, title_include, title_exclude, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (channel_name) DO UPDATE SET
                 display_name = excluded.display_name,
                 include_shorts = excluded.include_shorts,
                 hide_channel = excluded.hide_channel,
                 auto_watched_days = excluded.auto_watched_days,
                 title_include = excluded.title_include,
                 title_exclude = excluded.title_exclude,
                 updated_at = excluded.updated_at""",
            (channel, cols["display_name"], cols["include_shorts"],
             cols["hide_channel"], cols["auto_watched_days"],
             cols["title_include"], cols["title_exclude"], now),
        )


async def video_row(video_id):
    async with wytchr.standalone_db() as db:
        return await db.execute(
            "SELECT status, watched_at, status_changed_at FROM videos "
            "WHERE video_id = ?",
            (video_id,),
        ).fetchone()


async def board_html(client, query=""):
    resp = await client.get(f"/board{query}", headers=AUTH)
    assert resp.status_code == 200
    return (await resp.get_data()).decode()
