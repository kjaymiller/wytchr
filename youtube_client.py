"""YouTube Data API v3 helpers — async, httpx-backed.

Matches the rest of the codebase's HTTP style (httpx.AsyncClient) rather
than pulling in google-api-python-client, which is sync-only and would
need an asyncio.to_thread wrapper at every call site.

Not yet wired up to any call sites — the upcoming pivot steps swap the
existing yt-dlp resolve path and the ytdl-sub-api poll path over to
these helpers.
"""
from __future__ import annotations

import re
from typing import Iterable

import httpx

_API_BASE = "https://www.googleapis.com/youtube/v3"

_CHANNEL_ID_RE = re.compile(r"youtube\.com/channel/(UC[\w-]{22})", re.I)
_HANDLE_RE = re.compile(r"youtube\.com/@([\w.-]+)", re.I)
_VIDEO_ID_RE = re.compile(
    r"(?:v=|youtu\.be/|/shorts/|/embed/)([\w-]{11})", re.I
)


class YouTubeAPIError(RuntimeError):
    """Wraps non-200 responses + network errors with the status + body."""


async def _get(client: httpx.AsyncClient, path: str, params: dict, key: str) -> dict:
    params = {**params, "key": key}
    r = await client.get(f"{_API_BASE}/{path}", params=params, timeout=10.0)
    if r.status_code != 200:
        raise YouTubeAPIError(f"{path} -> {r.status_code}: {r.text[:200]}")
    return r.json()


async def resolve_channel(
    client: httpx.AsyncClient, url: str, *, api_key: str
) -> dict:
    """Resolve any YouTube URL shape to canonical channel metadata.

    Returns {channel_id, handle, title, url}. Raises YouTubeAPIError or
    ValueError if the URL can't be resolved.
    """
    # Cheap shortcut: explicit UC... id in the URL.
    m = _CHANNEL_ID_RE.search(url)
    if m:
        return await _channel_by_id(client, m.group(1), api_key=api_key)

    # Handle (@foo) — channels.list supports forHandle (param name uses
    # the literal `@`).
    m = _HANDLE_RE.search(url)
    if m:
        data = await _get(
            client,
            "channels",
            {"part": "snippet", "forHandle": f"@{m.group(1)}"},
            api_key,
        )
        items = data.get("items") or []
        if not items:
            raise ValueError(f"no channel for handle @{m.group(1)}")
        return _channel_from_item(items[0])

    # Video URL — look up the video's channelId, then the channel.
    m = _VIDEO_ID_RE.search(url)
    if m:
        data = await _get(
            client, "videos", {"part": "snippet", "id": m.group(1)}, api_key
        )
        items = data.get("items") or []
        if not items:
            raise ValueError(f"no video for id {m.group(1)}")
        channel_id = items[0]["snippet"]["channelId"]
        return await _channel_by_id(client, channel_id, api_key=api_key)

    raise ValueError(f"unrecognized YouTube URL shape: {url}")


async def _channel_by_id(
    client: httpx.AsyncClient, channel_id: str, *, api_key: str
) -> dict:
    data = await _get(
        client, "channels", {"part": "snippet", "id": channel_id}, api_key
    )
    items = data.get("items") or []
    if not items:
        raise ValueError(f"no channel for id {channel_id}")
    return _channel_from_item(items[0])


def _channel_from_item(item: dict) -> dict:
    snippet = item.get("snippet") or {}
    channel_id = item["id"]
    handle = (snippet.get("customUrl") or "").lstrip("@") or None
    return {
        "channel_id": channel_id,
        "handle": handle,
        "title": snippet.get("title"),
        "url": f"https://www.youtube.com/channel/{channel_id}",
    }


def uploads_playlist_id(channel_id: str) -> str:
    """Channel uploads playlist ID — UC... → UU..., no API call needed."""
    if not channel_id.startswith("UC"):
        raise ValueError(f"not a channel ID: {channel_id}")
    return "UU" + channel_id[2:]


async def list_channel_uploads(
    client: httpx.AsyncClient,
    channel_id: str,
    *,
    api_key: str,
    limit: int = 30,
) -> list[dict]:
    """Recent uploads for a channel. Returns up to `limit` items with
    keys: video_id, title, published_at, thumbnail_url.

    Pagination intentionally omitted — the homelab poll cadence wants
    "what's new in the last N," and N stays small.
    """
    playlist_id = uploads_playlist_id(channel_id)
    data = await _get(
        client,
        "playlistItems",
        {
            "part": "snippet",
            "playlistId": playlist_id,
            "maxResults": min(limit, 50),
        },
        api_key,
    )
    out: list[dict] = []
    for item in data.get("items") or []:
        s = item.get("snippet") or {}
        rid = s.get("resourceId") or {}
        thumbs = (s.get("thumbnails") or {})
        thumb = (thumbs.get("high") or thumbs.get("default") or {}).get("url")
        out.append(
            {
                "video_id": rid.get("videoId"),
                "title": s.get("title"),
                "published_at": s.get("publishedAt"),
                "thumbnail_url": thumb,
            }
        )
    return out


async def get_videos(
    client: httpx.AsyncClient,
    video_ids: Iterable[str],
    *,
    api_key: str,
) -> list[dict]:
    """Batch videos.list lookup — used to enrich metadata (duration,
    description) for the IDs returned by list_channel_uploads.

    Caller chunks to <= 50 IDs per call; this helper does one call.
    """
    ids = list(video_ids)
    if not ids:
        return []
    data = await _get(
        client,
        "videos",
        {"part": "snippet,contentDetails", "id": ",".join(ids)},
        api_key,
    )
    return data.get("items") or []
