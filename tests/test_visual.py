"""Visual tests: a real wytchr server, seeded data, a real browser.

The rest of this suite drives the app through Quart's test client, which
never runs a line of JavaScript. The board is an htmx page whose columns
arrive by an `hx-get` on load, so most of what a person sees is invisible
to it. These tests start hypercorn against the same scratch Postgres, seed
it with scripts/seed_fixtures.py and open each page in Chromium:

- every page renders, signed in, with no console errors or failed requests
- no page scrolls sideways at phone, tablet or desktop widths
- the signed-out redirect and the token login both work in a browser
- a full-page PNG of each page/viewport lands in .preview/playwright/
  (WYTCHR_SHOTS_DIR overrides), for a person or the factory to look at

No YouTube: the key is the dummy from conftest.py, and the seed stands in
for everything the API would fetch.

Browsers: `playwright` is pinned to the version the software factory's
workcell image installs (PLAYWRIGHT_VERSION in its workcell/Dockerfile), so
in a cell it uses the Chromium already at $PLAYWRIGHT_BROWSERS_PATH.
Locally, once: `uv run playwright install chromium`. With no browser these
tests skip, unless WYTCHR_VISUAL_REQUIRED=1 (CI sets it), where a missing
browser fails instead of passing quietly.

    mise run test:visual        # just these
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import os
import socket
import subprocess
import sys
import time
import urllib.request

import pytest

from conftest import ROOT, TOKEN, _reset_cache

pytestmark = pytest.mark.visual

SHOTS = ROOT / os.environ.get("WYTCHR_SHOTS_DIR", ".preview/playwright")

# The widths the stylesheets branch on, plus a desktop control (the same set
# as scripts/preview.py). 320 is the narrowest phone still in circulation.
VIEWPORTS = {
    "phone-320": (320, 700),
    "phone-390": (390, 844),
    "tablet-820": (820, 1180),
    "desktop-1440": (1440, 900),
}

# path -> a selector that only exists once the page has really rendered.
# For the board that means htmx ran and swapped the columns in.
PAGES = {
    "/": '[data-channel="Primer Labs"]',
    "/channels": "text=Kurzgesagt",
    "/channels/add": "form",
    "/channels/Kurzgesagt/settings": "form",
    "/favorites": "text=Simulating natural selection",
    "/tags": "text=to-watch",
    "/webhooks": "text=all-my-favs",
}


# ── server and browser ────────────────────────────────────────────────────


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_for(url: str, proc: subprocess.Popen, timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"wytchr exited with {proc.returncode} before answering")
        try:
            with urllib.request.urlopen(url, timeout=1):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"wytchr did not answer {url} within {timeout}s")


@pytest.fixture(scope="module")
def base_url():
    """Seed the scratch DB and serve wytchr on a free port for this module."""
    env = {**os.environ, "POLL_INTERVAL_MINUTES": "1440"}
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "seed_fixtures.py")],
        env=env, check=True, cwd=ROOT,
    )
    # The functional tests leave rendered board slices in Valkey; drop them
    # so the browser sees the seed, not the previous test's cards. In a
    # thread, because pytest-asyncio may already have a loop running here.
    with concurrent.futures.ThreadPoolExecutor(1) as pool:
        pool.submit(asyncio.run, _reset_cache()).result()

    SHOTS.mkdir(parents=True, exist_ok=True)
    port = _free_port()
    with open(SHOTS / "server.log", "w") as log:
        proc = subprocess.Popen(
            [sys.executable, "-m", "hypercorn", "app:app",
             "--bind", f"127.0.0.1:{port}", "--workers", "1"],
            cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
        )
        url = f"http://127.0.0.1:{port}"
        try:
            _wait_for(f"{url}/healthz", proc)
            yield url
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()


def _no_browser(reason: str):
    if os.environ.get("WYTCHR_VISUAL_REQUIRED") == "1":
        pytest.fail(reason, pytrace=False)
    pytest.skip(reason)


@pytest.fixture(scope="module")
def browser():
    try:
        from playwright.sync_api import Error, sync_playwright
    except ImportError:
        _no_browser("the playwright package is not installed (uv sync)")
    with sync_playwright() as pw:
        try:
            b = pw.chromium.launch(args=["--no-sandbox", "--disable-dev-shm-usage"])
        except Error as exc:
            _no_browser(
                f"no Chromium for playwright ({str(exc).splitlines()[0]}); "
                "run `uv run playwright install chromium`"
            )
        yield b
        b.close()


@pytest.fixture
def open_page(browser, base_url):
    """open_page(viewport="desktop-1440", authed=True) -> (page, errors).

    `errors` collects console errors, uncaught exceptions and failed
    requests: a missing htmx is a blank board, not a 500, so status codes
    alone would miss it."""
    contexts = []

    def _open(viewport: str = "desktop-1440", authed: bool = True):
        w, h = VIEWPORTS[viewport]
        ctx = browser.new_context(viewport={"width": w, "height": h}, base_url=base_url)
        if authed:
            ctx.add_cookies([{"name": "wytchr_token", "value": TOKEN, "url": base_url}])
        contexts.append(ctx)
        page = ctx.new_page()
        errors: list[str] = []
        page.on("console", lambda m: errors.append(m.text) if m.type == "error" else None)
        page.on("pageerror", lambda e: errors.append(str(e)))
        page.on("requestfailed", lambda r: errors.append(f"{r.url}: {r.failure}"))
        return page, errors

    yield _open
    for ctx in contexts:
        ctx.close()


OVERFLOW_JS = """(vw) => {
    const out = [];
    for (const el of document.querySelectorAll('body *')) {
        const r = el.getBoundingClientRect();
        if (r.width === 0 || r.height === 0) continue;
        let scroller = false;  // intentional horizontal scrollers are fine
        for (let p = el.parentElement; p; p = p.parentElement) {
            const o = getComputedStyle(p).overflowX;
            if (o === 'auto' || o === 'scroll') { scroller = true; break; }
        }
        if (scroller) continue;
        if (r.right > vw + 1 || r.left < -1) {
            out.push(el.tagName.toLowerCase()
                + (typeof el.className === 'string' && el.className.trim()
                   ? '.' + el.className.trim().split(/\\s+/).join('.') : '')
                + ` [${Math.round(r.left)}..${Math.round(r.right)}]`);
        }
    }
    return [...new Set(out)].slice(0, 6);
}"""


def overflow(page) -> list[str]:
    w = page.viewport_size["width"]
    found = page.evaluate(OVERFLOW_JS, w)
    doc = page.evaluate("document.documentElement.scrollWidth")
    if doc > w + 1:
        found.insert(0, f"document is {doc}px wide in a {w}px viewport")
    return found


def broken_images(page) -> list[str]:
    """<img> elements that finished loading with no pixels. A bad data: URI
    never fires `requestfailed`, so the error listener alone misses it."""
    return page.evaluate("""() => [...document.images]
        .filter(i => i.complete && i.naturalWidth === 0)
        .map(i => i.src.slice(0, 80))""")


def _slug(path: str) -> str:
    return path.strip("/").replace("/", "_") or "board"


# ── tests ─────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("viewport", VIEWPORTS)
@pytest.mark.parametrize("path", PAGES)
def test_page_renders_without_overflow(open_page, path, viewport):
    page, errors = open_page(viewport)
    resp = page.goto(path, wait_until="networkidle")

    assert resp.status == 200, f"{path} returned {resp.status}"
    assert not page.url.endswith("/login"), f"{path} bounced to the login page"
    page.locator(PAGES[path]).first.wait_for(state="attached", timeout=5000)
    page.screenshot(path=str(SHOTS / f"{_slug(path)}@{viewport}.png"), full_page=True)

    assert errors == [], f"{path}@{viewport}: {errors}"
    assert broken_images(page) == [], f"{path}@{viewport} broken <img>: {broken_images(page)}"
    assert overflow(page) == [], f"{path}@{viewport} scrolls sideways: {overflow(page)}"


def test_board_shows_seeded_columns_and_omits_hidden_cards(open_page):
    page, _ = open_page()
    page.goto("/", wait_until="networkidle")
    board = page.locator('[data-channel="Primer Labs"]')
    board.wait_for()

    assert page.locator("[data-channel]").count() >= 4
    # Hidden videos stay off the board until "show hidden" is on.
    assert page.locator(".badge.hidden").count() == 0


def test_poll_error_is_shown_on_the_roster(open_page):
    page, _ = open_page()
    page.goto("/channels", wait_until="networkidle")
    assert page.get_by_text("quotaExceeded").first.is_visible()


def test_signed_out_redirects_to_login(open_page):
    page, _ = open_page(authed=False)
    page.goto("/", wait_until="networkidle")
    assert page.url.endswith("/login")
    page.screenshot(path=str(SHOTS / "login@desktop-1440.png"), full_page=True)


def test_token_login_reaches_the_board(open_page):
    page, _ = open_page(authed=False)
    page.goto("/login")
    page.fill('input[name="token"]', TOKEN)
    page.locator('form [type="submit"], form button').first.click()
    page.wait_for_url("**/")
    page.locator('[data-channel="Primer Labs"]').wait_for()


def test_wrong_token_is_refused(open_page):
    page, _ = open_page(authed=False)
    page.goto("/login")
    page.fill('input[name="token"]', "not-the-token")
    page.locator('form [type="submit"], form button').first.click()
    page.get_by_text("invalid token").wait_for()
