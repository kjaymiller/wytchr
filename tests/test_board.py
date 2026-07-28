"""Board rendering — empty-channel suppression and the count that lies.

The headline regression: `actionable` came from a SQL COUNT that can't
see the title include/exclude regexes, while the rendered card list is
filtered in Python afterwards. So a channel showing one video offered to
"skip all 2". Fixed once, then very nearly reverted by a rebase, which
is exactly why it's pinned here.
"""

from conftest import AUTH, add_channel, add_video, board_html, set_settings


async def test_channel_with_no_videos_is_omitted(client):
    await add_channel("quiet")
    await add_channel("loud")
    await add_video("v1", "loud")

    html = await board_html(client)

    assert 'data-channel="loud"' in html
    assert 'data-channel="quiet"' not in html


async def test_channel_emptied_by_title_filter_is_omitted(client):
    """Emptiness has to be judged after the Python-side regex pass, not
    from the SQL count — the filtered-to-nothing case is the whole point."""
    await add_channel("chan")
    await set_settings("chan", title_exclude="sponsored")
    await add_video("v1", "chan", title="sponsored segment")

    html = await board_html(client)

    assert 'data-channel="chan"' not in html


async def test_profile_section_vanishes_when_all_its_channels_are_empty(client):
    await add_channel("quiet", preset="podcasts")
    await add_channel("loud", preset="music")
    await add_video("v1", "loud")

    html = await board_html(client)

    assert 'data-profile="music"' in html
    assert 'data-profile="podcasts"' not in html


async def test_actionable_count_matches_the_videos_actually_shown(client):
    """The regression this suite exists for.

    Two 'new' rows, one excluded by title. The header count, the confirm
    copy, and the card list must all say one — a count of 2 here means
    the SQL-count path is back.
    """
    await add_channel("chan")
    await set_settings("chan", title_exclude="sponsored")
    await add_video("v1", "chan", title="a real video")
    await add_video("v2", "chan", title="sponsored segment")

    html = await board_html(client)

    assert 'id="card-v1"' in html
    assert 'id="card-v2"' not in html
    assert "<strong>1 new</strong>" in html
    assert "<strong>2 new</strong>" not in html
    # The confirm copy reads off the same number.
    assert "2 unwatched" not in html


async def test_actionable_count_excludes_shorts_when_they_are_hidden(client):
    await add_channel("chan")
    await set_settings("chan", include_shorts=0)
    await add_video("v1", "chan", title="a long one")
    await add_video("v2", "chan", title="a short one", short=True)

    html = await board_html(client)

    assert "<strong>1 new</strong>" in html
    assert 'id="card-v2"' not in html


async def test_board_carries_no_totals_block(client):
    """Deliberately removed: totals were summed from the rendered
    columns, so hiding empty channels — and later serving columns from a
    cache — made them under-report. Roster-wide counts live on /channels.
    """
    await add_channel("chan")
    await add_video("v1", "chan")

    html = await board_html(client)

    assert 'class="stats"' not in html


async def test_hidden_view_shows_only_channels_with_hidden_videos(client):
    await add_channel("chan")
    await add_channel("other")
    await add_video("v1", "chan", status="hidden")
    await add_video("v2", "other")

    html = await board_html(client, "?hidden=1")

    assert 'data-channel="chan"' in html
    assert 'data-channel="other"' not in html


async def test_empty_board_explains_itself(client):
    """Channels exist, none have anything new — that's a different state
    from having no channels at all, and it should read differently."""
    await add_channel("chan")
    await add_video("v1", "chan", status="watched", watched_at=1)

    html = await board_html(client)

    assert "All caught up" in html


async def test_skip_all_removes_the_channel_from_the_board(client):
    """End to end: the two features in this release meeting each other."""
    await add_channel("chan")
    await add_video("v1", "chan")
    assert 'data-channel="chan"' in await board_html(client)

    await client.post("/channels/chan/skip-all", headers=AUTH)

    assert 'data-channel="chan"' not in await board_html(client)
    assert 'data-channel="chan"' in await board_html(client, "?hidden=1")
