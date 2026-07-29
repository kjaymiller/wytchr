"""Single-card re-renders — the htmx swap path.

Three routes hand back one freshly-rendered card instead of a board:
favorite, unhide, and fetch-description. They share `_render_card`,
which for three releases rendered `_card.html` with only `v=row` while
the template also reads `video_tags_map`. Jinja does not treat a missing
name as empty here — `video_tags_map.get(...)` on an Undefined raises,
so every one of those swaps 500'd and the card never updated in the UI.

The board renderers were unaffected because they build the map for the
whole page, which is exactly why this survived: the bug lived only on
the path no board test exercised. So these tests go through the routes
rather than calling `_render_card`, and assert on the rendered card —
a card that comes back tagless and 200 is still a regression.
"""

from conftest import AUTH, add_channel, add_video

HX = {**AUTH, "HX-Request": "true"}


async def add_tag(client, video_id, name):
    resp = await client.post(
        f"/videos/{video_id}/tags", form={"name": name}, headers=AUTH
    )
    assert resp.status_code == 200
    return (await resp.get_data()).decode()


async def seed(client, video_id="vid1", *, tag=None):
    await add_channel("chan")
    await add_video(video_id, "chan", title="a video")
    if tag:
        await add_tag(client, video_id, tag)
    return video_id


async def test_favorite_returns_the_card(client):
    video_id = await seed(client, tag="topic:python")

    resp = await client.post(f"/videos/{video_id}/favorite", headers=HX)

    assert resp.status_code == 200, "the favorite swap must not 500"
    html = (await resp.get_data()).decode()
    assert f"card-{video_id}" in html
    assert "python" in html, "the card dropped its tags"


async def test_unhide_returns_the_card(client):
    video_id = await seed(client, tag="topic:python")
    await client.post(f"/videos/{video_id}/hide", headers=HX)

    resp = await client.post(f"/videos/{video_id}/unhide", headers=HX)

    assert resp.status_code == 200
    html = (await resp.get_data()).decode()
    assert f"card-{video_id}" in html
    assert "python" in html


async def test_fetch_description_returns_the_card(client):
    # No YouTube key that resolves in tests, so the fetch yields nothing
    # and the route falls through to the same render. That is the point:
    # the 500 was in rendering, not in fetching.
    video_id = await seed(client, tag="topic:python")

    resp = await client.post(f"/videos/{video_id}/fetch-description", headers=HX)

    assert resp.status_code == 200
    html = (await resp.get_data()).decode()
    assert f"card-{video_id}" in html


async def test_untagged_card_still_renders(client):
    """The empty-map case. `_video_tags_map` short-circuits on an empty
    id list and returns {} — a card with no tags must render, not trip
    the same Undefined by a different route.
    """
    video_id = await seed(client)

    resp = await client.post(f"/videos/{video_id}/favorite", headers=HX)

    assert resp.status_code == 200
    assert f"card-{video_id}" in (await resp.get_data()).decode()
