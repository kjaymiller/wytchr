"""watch-all / skip-all — the semantics that separate the two piles.

The bug this file exists for: the original /mark-watched flipped every
'new' row in the channel, ignoring the per-channel filters the board
renders through. So "mark all watched" silently acted on videos the
operator had never been shown. Splitting it into watch/skip made that
worse, not better — a "skip all" that hides invisible videos is a
data-loss-shaped surprise.
"""

from conftest import (
    AUTH,
    add_channel,
    add_video,
    set_settings,
    video_row,
)

import app as wytchr


async def test_watch_all_marks_watched_and_stamps_watched_at(client):
    await add_channel("chan")
    await add_video("v1", "chan")

    resp = await client.post("/channels/chan/watch-all", headers=AUTH)
    assert resp.status_code == 200

    row = await video_row("v1")
    assert row["status"] == "watched"
    assert row["watched_at"] is not None


async def test_skip_all_hides_and_leaves_watched_at_null(client):
    """A skip is a dismissal, not a view.

    watched_at is what auto_mark_watched() and the watched archive read.
    Stamping it on a skip would file "never going to watch this" next to
    "watched this", and there'd be no way to tell them apart afterwards.
    """
    await add_channel("chan")
    await add_video("v1", "chan")

    resp = await client.post("/channels/chan/skip-all", headers=AUTH)
    assert resp.status_code == 200

    row = await video_row("v1")
    assert row["status"] == "hidden"
    assert row["watched_at"] is None


async def test_skipped_video_survives_the_auto_watched_sweep(client):
    """The consequence of the NULL above, stated as behaviour.

    auto_mark_watched() claims rows with watched_at IS NULL AND
    status = 'new'. A skipped row is 'hidden', so the sweep must leave it
    alone — otherwise a skip silently becomes a watch an hour later.
    """
    await add_channel("chan")
    await set_settings("chan", auto_watched_days=1)
    # seen well before the 1-day cutoff, so the sweep would claim it if
    # status let it.
    await add_video("v1", "chan", seen_at=1)

    await client.post("/channels/chan/skip-all", headers=AUTH)
    await wytchr.auto_mark_watched()

    row = await video_row("v1")
    assert row["status"] == "hidden"
    assert row["watched_at"] is None


async def test_bulk_actions_ignore_shorts_the_board_excludes(client):
    """include_shorts=0 means shorts aren't rendered — so a bulk action
    must not reach them either."""
    await add_channel("chan")
    await set_settings("chan", include_shorts=0)
    await add_video("v1", "chan", title="a long one")
    await add_video("v2", "chan", title="a short one", short=True)

    await client.post("/channels/chan/skip-all", headers=AUTH)

    assert (await video_row("v1"))["status"] == "hidden"
    assert (await video_row("v2"))["status"] == "new", (
        "the short was never shown on the board; skip-all must not touch it"
    )


async def test_bulk_actions_respect_title_exclude(client):
    await add_channel("chan")
    await set_settings("chan", title_exclude="sponsored")
    await add_video("v1", "chan", title="a real video")
    await add_video("v2", "chan", title="sponsored segment")

    await client.post("/channels/chan/watch-all", headers=AUTH)

    assert (await video_row("v1"))["status"] == "watched"
    assert (await video_row("v2"))["status"] == "new"


async def test_bulk_actions_respect_title_include(client):
    await add_channel("chan")
    await set_settings("chan", title_include="ep\\.")
    await add_video("v1", "chan", title="ep. 12 — the good one")
    await add_video("v2", "chan", title="a trailer")

    await client.post("/channels/chan/skip-all", headers=AUTH)

    assert (await video_row("v1"))["status"] == "hidden"
    assert (await video_row("v2"))["status"] == "new"


async def test_bulk_actions_stay_within_their_channel(client):
    await add_channel("chan")
    await add_channel("other")
    await add_video("v1", "chan")
    await add_video("v2", "other")

    await client.post("/channels/chan/skip-all", headers=AUTH)

    assert (await video_row("v1"))["status"] == "hidden"
    assert (await video_row("v2"))["status"] == "new"


async def test_bulk_actions_only_claim_new_videos(client):
    """An already-watched video must not be dragged into the hidden pile
    by a later skip-all."""
    await add_channel("chan")
    await add_video("v1", "chan", status="watched", watched_at=123)

    await client.post("/channels/chan/skip-all", headers=AUTH)

    row = await video_row("v1")
    assert row["status"] == "watched"
    assert row["watched_at"] == 123


async def test_unknown_channel_404s(client):
    for action in ("watch-all", "skip-all"):
        resp = await client.post(f"/channels/nope/{action}", headers=AUTH)
        assert resp.status_code == 404, action


async def test_bulk_actions_require_auth(client):
    await add_channel("chan")
    await add_video("v1", "chan")

    resp = await client.post("/channels/chan/skip-all")
    assert resp.status_code == 401
    assert (await video_row("v1"))["status"] == "new"


async def test_htmx_bulk_response_reports_the_action(client):
    """The toast reads off this trigger payload, and it has been wrong
    before — the old event name had no listener at all."""
    await add_channel("chan")
    await add_video("v1", "chan")

    resp = await client.post(
        "/channels/chan/skip-all", headers={**AUTH, "HX-Request": "true"}
    )
    assert resp.status_code == 200
    trigger = resp.headers["HX-Trigger"]
    assert "wytchr:channel-bulk" in trigger
    assert '"action": "skip"' in trigger or '"action":"skip"' in trigger
    assert '"marked": 1' in trigger or '"marked":1' in trigger
