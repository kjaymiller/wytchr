"""The manage-channels page.

It inherited the roster-wide totals the board gave up, so it is now the
only place those numbers are stated. It must count every channel — a
page that hides the quiet ones would defeat the reason it was added,
which is that the board no longer shows them.
"""

import re

from conftest import AUTH, add_channel, add_video, set_settings


async def page(client):
    resp = await client.get("/channels", headers=AUTH)
    assert resp.status_code == 200
    return (await resp.get_data()).decode()


async def test_lists_channels_the_board_omits(client):
    await add_channel("quiet")
    await add_channel("hidden-one")
    await set_settings("hidden-one", hide_channel=1)

    html = await page(client)

    assert "quiet" in html
    assert "hidden-one" in html


async def test_counts_honour_include_shorts(client):
    await add_channel("noshorts")
    await set_settings("noshorts", include_shorts=0)
    await add_video("v1", "noshorts", title="long")
    await add_video("v2", "noshorts", title="short", short=True)

    await add_channel("withshorts")
    await set_settings("withshorts", include_shorts=1)
    await add_video("v3", "withshorts", title="long")
    await add_video("v4", "withshorts", title="short", short=True)

    html = await page(client)

    # Both channels hold two rows. data-new is the sort key the page
    # exposes per row, so it pins the count without depending on how the
    # counts line is laid out.
    rows = dict(re.findall(r'data-name="([^"]+)"[^>]*?data-new="(\d+)"', html, re.S))
    assert rows["noshorts"] == "1", "the short should not be counted"
    assert rows["withshorts"] == "2"


async def test_totals_count_every_channel(client):
    """Including hidden and zero-video ones — this is the roster, not a
    view of it."""
    await add_channel("a")
    await add_channel("b")
    await set_settings("b", hide_channel=1)
    await add_channel("empty")
    await add_video("v1", "a")
    await add_video("v2", "b")

    html = await page(client)

    assert "<strong>3</strong> channels" in html
    assert "<strong>2</strong> videos" in html


async def test_requires_auth(client):
    resp = await client.get("/channels")
    assert resp.status_code in (302, 401)


async def test_surfaces_poll_failures(client):
    await add_channel("broken")
    from conftest import wytchr

    async with wytchr.standalone_db() as db:
        await db.execute(
            "UPDATE channels SET last_error = ? WHERE name = ?",
            ("quota exceeded", "broken"),
        )

    html = await page(client)

    assert "quota exceeded" in html


async def test_never_polled_channel_renders(client):
    await add_channel("fresh", polled=False)

    html = await page(client)

    assert "never" in html
