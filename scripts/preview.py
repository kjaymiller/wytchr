#!/usr/bin/env python3
"""Render every wytchr template with fixture data — no database required.

The functional suite in tests/ drives the real app against a real Postgres,
which is the right call for behaviour but makes it awkward to eyeball layout.
This renders the templates directly with stand-in context so you can look at
the chrome, and screenshot it at the widths the CSS actually branches on.

    mise run preview     # serve the rendered pages on :8899
    mise run shots       # screenshot every page at 3 widths + flag overflow

`--shots` needs the Playwright browser, a one-time download:

    uv run --with-requirements requirements-dev.txt playwright install chromium

The overflow check is the point of the whole thing: a page whose content is
wider than the viewport is the specific failure that makes a site unusable on
a phone, and it is easy to reintroduce by accident.
"""

from __future__ import annotations

import argparse
import functools
import http.server
import pathlib
import shutil
import socketserver
import sys
import threading
import types

from jinja2 import Environment, FileSystemLoader

ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATES = ROOT / "templates"
STATIC = ROOT / "static"
OUT = ROOT / ".preview"
PORT = 8899

# The widths the stylesheets branch on, plus a desktop control. 320 is the
# narrowest phone still in circulation (iPhone SE); if it survives that it
# survives anything.
VIEWPORTS = [("phone-320", 320, 700), ("phone-390", 390, 844),
             ("tablet-820", 820, 1180), ("desktop-1440", 1440, 900)]

# Inline SVG so the preview renders identically with no network.
THUMB = (
    "data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' "
    "width='320' height='180'><rect width='320' height='180' fill='%23241c2e'/>"
    "<text x='160' y='96' fill='%236a5f6a' font-size='18' text-anchor='middle'"
    " font-family='sans-serif'>thumb</text></svg>"
)


def _video(i: int, status: str = "new", favorited: bool = False) -> dict:
    return dict(
        video_id=f"vid{i}",
        title=f"A fairly long video title number {i} that has to wrap somewhere",
        url="https://youtu.be/x",
        thumbnail_url=THUMB,
        description="A description that runs on long enough to clamp at three lines.",
        upload_date="20260715",
        duration=754,
        status=status,
        favorited_at=1 if favorited else None,
        channel_name="Channel One",
    )


def _column(name: str, display: str, n: int, err: str | None = None) -> dict:
    return dict(
        channel=dict(name=name, display_name=display, url="https://youtube.com/@x",
                     feed_url="https://youtube.com/feeds/x", last_error=err),
        counts={"watched": 12, "hidden": 3},
        actionable=n,
        videos=[_video(i, favorited=(i == 2)) for i in range(1, n + 1)],
    )


def fixtures() -> tuple[dict, dict]:
    """Return (board_context, {page_name: context}) for every template."""
    tags = [dict(name="topic:python", suffix="python", video_count=4, channel_count=1),
            dict(name="topic:rust", suffix="rust", video_count=2, channel_count=0),
            dict(name="solo", suffix="solo", video_count=1, channel_count=1)]

    board = dict(
        sections=[
            dict(profile="podcasts",
                 columns=[_column("chan-one", "Channel One With A Rather Long Name", 4),
                          _column("chan-two", "Chan Two", 2,
                                  err="quota exceeded on last poll")]),
            dict(profile="", columns=[_column("chan-three", "Third Channel", 3)]),
        ],
        all_tags=tags, active_tag=None,
        grouped_tags={"topic": tags[:2], "": tags[2:]},
        show_hidden=False, has_channels=True,
        channel_tags_map={"chan-one": ["topic:python"]},
        video_tags_map={"vid1": ["topic:python", "topic:rust"]},
    )

    channels = [
        dict(name="chan-one", display_name="Channel One With A Rather Long Name",
             preset="podcasts", aliased=True, url="https://youtube.com/@x", new=4,
             watched=12, hidden=3, hide_channel=0, auto_watched_days=14,
             include_shorts=1, last_polled="2h ago", last_polled_at=1, last_error=None),
        dict(name="chan-two", display_name="Chan Two", preset="", aliased=False,
             url="https://youtube.com/@y", new=0, watched=3, hidden=0, hide_channel=1,
             auto_watched_days=None, include_shorts=0, last_polled="never",
             last_polled_at=0,
             last_error="HTTP 403 quota exceeded from the YouTube Data API"),
    ]

    pages = {
        "channels.html": dict(channels=channels,
                              totals=dict(channels=3, videos=42, new=4, watched=15,
                                          hidden_channels=1, erroring=1)),
        "favorites.html": dict(videos=[_video(i, favorited=True) for i in range(1, 7)]),
        "tags.html": dict(grouped={"topic": tags[:2], "": tags[2:]}, flash="renamed."),
        "webhooks.html": dict(
            hooks=[dict(id=1, name="all-my-favs", event="video.favorited",
                        url="https://all-my-favs.example/wytchr/inbound/hook",
                        enabled=1, last_fired_at=1750000000, last_status=200,
                        last_error=None)],
            always_favs=True),
        "channel_settings.html": dict(
            channel=dict(name="chan-one", preset="podcasts"),
            settings=dict(display_name="Channel One", include_shorts=1, hide_channel=0,
                          auto_watched_days=14, title_include="", title_exclude=""),
            feed_url="https://www.youtube.com/feeds/videos.xml?channel_id=UC123",
            saved=True),
        "channel_add.html": dict(profiles=["podcasts", "music"], prefill_url="",
                                 error=None, always_favs=True),
        "login.html": dict(error="invalid token"),
    }
    return board, pages


def render() -> list[str]:
    """Render every page into OUT and return the page filenames."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES)))
    board_ctx, pages = fixtures()

    if OUT.exists():
        shutil.rmtree(OUT)
    OUT.mkdir(parents=True)

    # The board shell fetches its rows over HTMX; splice the partial in so the
    # static preview shows a populated board rather than "loading…".
    inner = env.get_template("_board.html").render(**board_ctx)
    shell = env.get_template("board.html").render(
        poll_interval=15, profiles=["podcasts", "music"], app_version="preview",
        request=types.SimpleNamespace(query_string=b""))
    (OUT / "board.html").write_text(shell.replace("      loading…", inner))

    for name, ctx in pages.items():
        (OUT / name).write_text(env.get_template(name).render(app_version="preview", **ctx))

    shutil.copytree(STATIC, OUT / "static")
    return ["board.html"] + list(pages)


def serve_forever(pages: list[str]) -> None:
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(OUT))
    with socketserver.TCPServer(("127.0.0.1", PORT), handler) as httpd:
        print(f"serving {OUT} on http://127.0.0.1:{PORT}")
        for p in pages:
            print(f"  http://127.0.0.1:{PORT}/{p}")
        print("ctrl-c to stop")
        httpd.serve_forever()


def shoot(pages: list[str]) -> int:
    """Screenshot every page at every viewport. Returns an exit code."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("playwright is not installed — `uv run --with-requirements "
              "requirements-dev.txt python scripts/preview.py --shots`", file=sys.stderr)
        return 2

    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(OUT))
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer(("127.0.0.1", PORT), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()

    shots = OUT / "shots"
    shots.mkdir(exist_ok=True)
    failures: list[str] = []

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch()
            for label, w, h in VIEWPORTS:
                ctx = browser.new_context(viewport={"width": w, "height": h},
                                          device_scale_factor=2)
                page = ctx.new_page()
                for name in pages:
                    page.goto(f"http://127.0.0.1:{PORT}/{name}", wait_until="load")
                    stem = name.removesuffix(".html")
                    page.screenshot(path=str(shots / f"{stem}@{label}.png"),
                                    full_page=True)
                    # Anything wider than the viewport means a sideways scroll.
                    overflow = page.evaluate(
                        """(vw) => {
                            const out = [];
                            for (const el of document.querySelectorAll('body *')) {
                                const r = el.getBoundingClientRect();
                                if (r.width === 0 || r.height === 0) continue;
                                // Ignore intentional horizontal scrollers.
                                let sc = false;
                                for (let p = el.parentElement; p; p = p.parentElement) {
                                    const o = getComputedStyle(p).overflowX;
                                    if (o === 'auto' || o === 'scroll') { sc = true; break; }
                                }
                                if (sc) continue;
                                if (r.right > vw + 1 || r.left < -1) {
                                    out.push(el.tagName.toLowerCase()
                                        + (el.className && typeof el.className === 'string'
                                           ? '.' + el.className.trim().split(/\\s+/).join('.')
                                           : '')
                                        + ` [${Math.round(r.left)}..${Math.round(r.right)}]`);
                                }
                            }
                            return [...new Set(out)].slice(0, 6);
                        }""", w)
                    doc_w = page.evaluate("document.documentElement.scrollWidth")
                    flag = ""
                    if overflow or doc_w > w + 1:
                        flag = f"  OVERFLOW doc={doc_w}px " + "; ".join(overflow)
                        failures.append(f"{stem}@{label}{flag}")
                    print(f"  {stem}@{label}{flag}")
                ctx.close()
            browser.close()
    finally:
        httpd.shutdown()

    print(f"\nscreenshots: {shots}")
    if failures:
        print(f"\n{len(failures)} page/viewport combos overflow horizontally:")
        for f in failures:
            print(f"  {f}")
        return 1
    print("no horizontal overflow at any viewport")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--serve", action="store_true", help=f"serve on :{PORT}")
    ap.add_argument("--shots", action="store_true",
                    help="screenshot each page at each viewport and check overflow")
    args = ap.parse_args()

    pages = render()
    print(f"rendered {len(pages)} pages into {OUT}")
    if args.shots:
        return shoot(pages)
    if args.serve:
        serve_forever(pages)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
