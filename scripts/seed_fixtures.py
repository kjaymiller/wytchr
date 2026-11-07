#!/usr/bin/env python3
"""Seed a scratch database with a board's worth of demo data — no YouTube.

Used by the software factory's screenshot buildfile (factory/visual.sh)
and by the Playwright suite (tests/visual/). Both need every page to
render with something on it, and neither may call the YouTube Data API:
the cells have no real key, and a screenshot that depends on what a
channel uploaded today is not evidence of anything.

It WIPES every wytchr table first. It refuses to run unless DATABASE_URL
points at localhost or a known throwaway host, so it can't be pointed at
prod by accident.

    DATABASE_URL=postgres://wytchr:wytchr@127.0.0.1:5433/wytchr \
        uv run python scripts/seed_fixtures.py
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import sys
import time
from urllib.parse import urlparse

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# app.py exits at import without these. None of them reach the network
# here: the seed only talks to Postgres.
os.environ.setdefault("API_TOKEN", "test-token")
os.environ.setdefault("YOUTUBE_API_KEY", "AIza" + "0" * 35)
os.environ.setdefault("VALKEY_URL", "redis://127.0.0.1:6379/0")
os.environ.setdefault("DATABASE_URL", "postgres://wytchr:wytchr@127.0.0.1:5433/wytchr")

# Hosts a scratch database lives on: local dev, the CI service container,
# the factory's docker sidecar (`db`) and the compose test stack (`pg`).
SAFE_HOSTS = {"127.0.0.1", "localhost", "::1", "db", "pg", "postgres"}

import app as wytchr  # noqa: E402

TABLES = (
    "video_tags", "channel_tags", "tags", "videos",
    "channel_settings", "channels", "webhooks", "app_settings",
)


# Thumbnail backgrounds. Every `#` in the SVG is written %23: a bare `#` in
# a data: URI starts the fragment, truncating the SVG to a broken image
# (test_visual.py's broken_images check catches it).
_SHADES = ("241c2e", "1f2a36", "2e1f24", "1f2e26", "2b2a1c", "2a1f33")


def _thumb(label: str, shade: int) -> str:
    """Inline SVG thumbnail, so the page renders the same with no network."""
    return (
        "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' "
        f"width='320' height='180'><rect width='320' height='180' fill='%23{_SHADES[shade % len(_SHADES)]}'/>"
        f"<text x='160' y='98' fill='%23cfc6cf' font-size='20' text-anchor='middle' "
        f"font-family='sans-serif'>{label}</text></svg>"
    )


# (name, preset, display_name, last_error)
CHANNELS = [
    ("Primer Labs", "science", "Primer", None),
    ("Kurzgesagt", "science", None, None),
    ("Tiny Desk", "music", None, None),
    ("Lofi Girl", "music", None, None),
    ("Bon Appetit", "", None, "playlistItems -> 403: quotaExceeded"),
    ("Long Name Channel That Keeps Going And Going", "", None, None),
]

VIDEO_TITLES = [
    "Simulating natural selection",
    "A fairly long video title that has to wrap somewhere on a narrow phone screen",
    "The egg",
    "Live from the office",
    "beats to relax/study to",
    "Every kitchen tool, ranked",
    "Why this is the last video in the column",
]


async def seed() -> None:
    host = urlparse(wytchr.DATABASE_URL).hostname or ""
    if host not in SAFE_HOSTS and os.environ.get("WYTCHR_SEED_ANY_HOST") != "1":
        sys.exit(
            f"refusing to wipe {host!r}: not a throwaway database host "
            f"({', '.join(sorted(SAFE_HOSTS))}). Set WYTCHR_SEED_ANY_HOST=1 to override."
        )

    await wytchr.init_db()
    now = int(time.time())
    async with wytchr.standalone_db() as db:
        for t in TABLES:
            await db.execute(f"TRUNCATE {t} CASCADE")

        for i, (name, preset, display, err) in enumerate(CHANNELS):
            await db.execute(
                "INSERT INTO channels (name, url, preset, last_polled_at, last_error) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, f"https://www.youtube.com/channel/UC{i:0>22}", preset,
                 now - 600 * (i + 1), err),
            )
            if display:
                await db.execute(
                    "INSERT INTO channel_settings (channel_name, display_name, updated_at) "
                    "VALUES (?, ?, ?)",
                    (name, display, now),
                )

        n = 0
        for ci, (name, *_rest) in enumerate(CHANNELS):
            per = 2 + (ci % 4)  # uneven columns, like a real board
            for j in range(per):
                n += 1
                vid = f"vid{n:08d}"  # 11 chars, the shape YouTube ids have
                short = ci == 3 and j == 0
                status = "hidden" if j == per - 1 and ci % 2 else "new"
                await db.execute(
                    """INSERT INTO videos (video_id, channel_name, title, duration,
                           upload_date, thumbnail_url, url, status, seen_at,
                           status_changed_at, favorited_at, description)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        vid, name, VIDEO_TITLES[(n - 1) % len(VIDEO_TITLES)],
                        45 if short else 60 * (3 + n) + n,
                        f"2026{(n % 12) + 1:02d}{(n % 27) + 1:02d}",
                        _thumb(f"%23{n}", n),
                        f"https://www.youtube.com/{'shorts/' if short else 'watch?v='}{vid}",
                        status, now - 3600 * n, now - 3600 * n,
                        now - 60 * n if n in (1, 4, 9) else None,
                        "A seeded description so the card shows one." if n % 3 == 0 else None,
                    ),
                )

        for tag in ("sport:mlb", "to-watch", "cooking"):
            await db.execute("INSERT INTO tags (name) VALUES (?)", (tag,))
        tag_id = {
            r["name"]: r["id"]
            for r in await db.execute("SELECT id, name FROM tags").fetchall()
        }
        await db.execute(
            "INSERT INTO video_tags (video_id, tag_id) VALUES (?, ?)",
            ("vid00000001", tag_id["to-watch"]),
        )
        await db.execute(
            "INSERT INTO channel_tags (channel_name, tag_id) VALUES (?, ?)",
            ("Bon Appetit", tag_id["cooking"]),
        )
        await db.execute(
            "INSERT INTO webhooks (name, url, event, enabled, created_at, last_fired_at, last_status) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("all-my-favs", "https://all-my-favs.example/wytchr", "video.favorited",
             1, now - 86400, now - 120, 200),
        )
    print(f"seeded {len(CHANNELS)} channels, {n} videos into {host}")


if __name__ == "__main__":
    asyncio.run(seed())
