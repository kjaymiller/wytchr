"""Board read cache: TTL derivation, invalidation, and degradation.

The rest of the suite proves the board renders correctly; this file
proves the cache in front of it doesn't change that answer. Three things
can go wrong with a read cache, and each gets a test here:

  1. the TTL is wrong           → _slice_ttl unit tests
  2. a write doesn't invalidate → stale cards survive an operator click
  3. Valkey dies mid-request    → the board 500s instead of reading through

(2) and (3) are the ones that bite in production.
"""

import os

import pytest

from conftest import (
    AUTH,
    add_channel,
    add_video,
    board_html,
    set_settings,
    valkey_asyncio,
    video_row,
    wytchr,
)

pytestmark = pytest.mark.skipif(
    valkey_asyncio is None or not os.environ.get("VALKEY_URL"),
    reason="board cache tests need VALKEY_URL and the valkey package",
)

POLL_SECS = wytchr.POLL_INTERVAL_MINUTES * 60


# --- TTL derivation ---------------------------------------------------
# Pure function, so these are cheap and exhaustive. The formula is
# "seconds until this channel's next poll", with rule-driven overrides.


def test_never_polled_channel_stays_short():
    """No last_polled_at means a poll is imminent — don't hold a slice
    for half an hour on a channel about to change."""
    assert wytchr._slice_ttl(None, {}, now=1_000_000) == wytchr.CACHE_TTL_POLL_DUE


def test_overdue_poll_stays_short():
    now = 1_000_000
    polled = now - POLL_SECS - 60  # a minute past due
    assert wytchr._slice_ttl(polled, {}, now=now) == wytchr.CACHE_TTL_POLL_DUE


def test_ttl_is_time_until_the_next_poll():
    """The upper bound that matters: a channel's videos can't change on
    their own before its next poll lands."""
    now = 1_000_000
    polled = now - 600  # 10 min into a 30 min cycle
    assert wytchr._slice_ttl(polled, {}, now=now) == POLL_SECS - 600


def test_hidden_channel_holds_its_slice():
    """A hidden channel isn't on the default board; only an explicit
    unhide changes what it shows, and that path invalidates."""
    now = 1_000_000
    polled = now - 600
    assert (
        wytchr._slice_ttl(polled, {"hide_channel": 1}, now=now)
        == wytchr.CACHE_TTL_MAX
    )


def test_auto_watched_channel_caps_at_one_sweep():
    """auto_mark_watched() runs hourly. Capping here means a missed
    invalidation self-heals within one sweep instead of riding to the
    ceiling."""
    now = 1_000_000
    polled = now - 600
    ttl = wytchr._slice_ttl(
        polled, {"hide_channel": 1, "auto_watched_days": 7}, now=now
    )
    assert ttl == 3600


def test_ttl_never_drops_below_the_floor():
    """A 1-second TTL would make the cache pure overhead."""
    now = 1_000_000
    polled = now - POLL_SECS + 2  # 2s left on the cycle
    assert wytchr._slice_ttl(polled, {}, now=now) >= wytchr.CACHE_TTL_MIN


# --- helpers ----------------------------------------------------------


async def cache_keys():
    client = valkey_asyncio.Valkey.from_url(os.environ["VALKEY_URL"])
    try:
        return [
            k
            async for k in client.scan_iter(
                match=f"{wytchr.CACHE_PREFIX}:*", count=500
            )
        ]
    finally:
        await client.aclose()


# --- caching actually happens ----------------------------------------


async def test_rendering_the_board_populates_the_cache(client):
    await add_channel("chan")
    await add_video("v1", "chan")

    assert await cache_keys() == []
    await board_html(client)
    assert await cache_keys(), "board render left nothing in the cache"


async def test_the_cache_is_actually_read(client):
    """Write straight to Postgres behind the app's back. The board must
    still show the cached answer — otherwise the cache isn't being read
    and every other test here proves nothing.
    """
    await add_channel("chan")
    await add_video("v1", "chan", title="original title")
    assert "original title" in await board_html(client)

    async with wytchr.standalone_db() as db:
        await db.execute(
            "UPDATE videos SET title = ? WHERE video_id = ?",
            ("changed behind the cache", "v1"),
        )

    html = await board_html(client)
    assert "original title" in html
    assert "changed behind the cache" not in html


# --- invalidation -----------------------------------------------------


async def test_watching_a_video_invalidates_its_channel(client):
    """The regression that would hurt most: operator clicks watch, card
    stays on the board until the TTL expires."""
    await add_channel("chan")
    await add_video("v1", "chan")
    assert 'id="card-v1"' in await board_html(client)

    resp = await client.post("/videos/v1/watched", headers=AUTH)
    assert resp.status_code == 200

    assert 'id="card-v1"' not in await board_html(client)


async def test_bulk_watch_invalidates_its_channel(client):
    await add_channel("chan")
    await add_video("v1", "chan")
    await add_video("v2", "chan")
    assert 'id="card-v1"' in await board_html(client)

    resp = await client.post("/channels/chan/watch-all", headers=AUTH)
    assert resp.status_code == 200

    html = await board_html(client)
    assert 'id="card-v1"' not in html
    assert 'id="card-v2"' not in html


async def test_editing_channel_settings_changes_the_slice(client):
    """Filter edits move the *key* rather than invalidating — a changed
    hash means the old entry is never read again. Same observable
    result, so assert on the board, not the mechanism."""
    await add_channel("chan")
    await add_video("v1", "chan", title="a real video")
    await add_video("v2", "chan", title="sponsored segment")
    assert 'id="card-v2"' in await board_html(client)

    await set_settings("chan", title_exclude="sponsored")

    html = await board_html(client)
    assert 'id="card-v1"' in html
    assert 'id="card-v2"' not in html


# --- degradation ------------------------------------------------------


async def test_board_reads_through_when_valkey_is_unreachable(
    client, monkeypatch
):
    """The documented failure policy: boot-required, runtime-forgiving.

    Everything cached is derived from Postgres with no write-back, so a
    Valkey error is indistinguishable from a miss — reading through is
    correct, just slower. If this test starts failing, someone changed
    _cache_degrade to `raise` and turned a cache blip into an outage.
    """
    await add_channel("chan")
    await add_video("v1", "chan", title="still rendered")

    dead = valkey_asyncio.Valkey.from_url(
        "redis://127.0.0.1:1/0", socket_connect_timeout=0.05
    )
    monkeypatch.setattr(wytchr, "_valkey", dead)
    monkeypatch.setattr(wytchr, "_cache_last_warn", 0.0)

    html = await board_html(client)
    assert 'id="card-v1"' in html
    assert "still rendered" in html

    await dead.aclose()


async def test_writes_still_succeed_when_valkey_is_unreachable(
    client, monkeypatch
):
    """Write routes invalidate *after* the commit has landed. Failing the
    request on a cache error would report failure for a write that
    actually succeeded."""
    await add_channel("chan")
    await add_video("v1", "chan")

    dead = valkey_asyncio.Valkey.from_url(
        "redis://127.0.0.1:1/0", socket_connect_timeout=0.05
    )
    monkeypatch.setattr(wytchr, "_valkey", dead)
    monkeypatch.setattr(wytchr, "_cache_last_warn", 0.0)

    resp = await client.post("/videos/v1/watched", headers=AUTH)
    assert resp.status_code == 200

    await dead.aclose()

    assert (await video_row("v1"))["status"] == "watched"
