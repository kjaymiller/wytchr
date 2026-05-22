"""Stub ytdl-sub-api for the test compose stack.

Implements just enough of the surface that wytchr touches:
  GET  /presets       -> base_preset + profiles
  GET  /channels      -> current registry
  POST /channels      -> register (validates url+name; rejects dups)
  POST /videos        -> always returns exit_code=0

Channel registry is in-memory; restart resets state.
"""
from __future__ import annotations

import os
from quart import Quart, jsonify, request

app = Quart(__name__)
TOKEN = os.environ.get("API_TOKEN", "test-token")

_channels: dict[str, dict] = {}
_PRESETS = {
    "base_preset": "TV",
    "profiles": ["recent", "archive"],
    "profile_details": {
        "recent": {
            "parents": ["TV"],
            "overrides": {"only_recent_date_range": "14days", "only_recent_max_files": 10},
        },
        "archive": {"parents": ["TV"], "overrides": {}},
    },
}


def _authed() -> bool:
    return request.headers.get("Authorization") == f"Bearer {TOKEN}"


@app.before_request
async def _gate():
    if request.path == "/healthz":
        return None
    if not _authed():
        return jsonify({"error": "unauthorized"}), 401


@app.get("/healthz")
async def healthz():
    return jsonify({"ok": True})


@app.get("/presets")
async def presets():
    return jsonify(_PRESETS)


@app.get("/channels")
async def list_channels():
    return jsonify({"channels": list(_channels.values())})


@app.post("/channels")
async def add_channel():
    body = await request.get_json(silent=True) or {}
    name = (body.get("name") or "").strip()
    url = (body.get("url") or "").strip()
    profile = (body.get("profile") or "").strip()
    if not name or not url:
        return jsonify({"error": "name and url required"}), 400
    if name in _channels:
        return jsonify({"error": f"channel {name} already exists"}), 409
    preset = f"{_PRESETS['base_preset']} | {profile}" if profile else _PRESETS["base_preset"]
    _channels[name] = {"name": name, "url": url, "preset": preset}
    return jsonify({"ok": True}), 201


@app.delete("/channels/<name>")
async def delete_channel(name: str):
    _channels.pop(name, None)
    return jsonify({"ok": True})


@app.post("/videos")
async def fake_download():
    return jsonify({"exit_code": 0, "output_tail": "stub download ok"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
