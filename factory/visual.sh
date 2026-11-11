#!/bin/sh
# The software factory's buildfile for screenshot evidence on pull requests
# (https://github.com/kjaymiller/factory). factory/visual.sh is the conventional
# path, so the factory finds it with no [visual] `script` key. It asks which
# steps exist and runs each one inside the workcell, from the checkout root:
#
#   steps   print the steps this file provides (this list)
#   setup   create the schema in the cell's scratch Postgres and seed demo data
#   serve   run wytchr on 127.0.0.1:8000 in the foreground; the factory
#           backgrounds it and waits for the URL to answer
#   auth    print one line, AUTH=name=value: the session cookie, so pages
#           behind the login are photographed as themselves
#
# Test credentials, none of them secret:
#   API_TOKEN=test-token        wytchr's own login (the wytchr_token cookie)
#   YOUTUBE_API_KEY=AIza000...  passes the boot check; YouTube rejects it. The
#                               seed data stands in for everything the API
#                               would fetch, so no page needs a real key.
#
# The cell sets DATABASE_URL (and VALKEY_URL on the docker backend). Without
# Valkey the app still boots and reads straight through to Postgres.
#
# In the factory's factory.toml this path is extra_high: every change here is a
# pull request a person reads.
set -eu

export API_TOKEN="${API_TOKEN:-test-token}"
export YOUTUBE_API_KEY="${YOUTUBE_API_KEY:-AIza00000000000000000000000000000000000}"
export DATABASE_URL="${DATABASE_URL:-postgres://wytchr:wytchr@127.0.0.1:5433/wytchr}"
export VALKEY_URL="${VALKEY_URL:-redis://127.0.0.1:6379/0}"
# The scheduler's jobs start paused, but keep the interval long anyway: a
# screenshot run must never poll YouTube.
export POLL_INTERVAL_MINUTES=1440

case "${1:-}" in
steps)
    printf '%s\n' setup serve auth
    ;;
setup)
    uv run python scripts/seed_fixtures.py
    ;;
serve)
    exec uv run hypercorn app:app --bind 127.0.0.1:8000 --workers 1
    ;;
auth)
    # wytchr has no users: the login form compares a token against API_TOKEN
    # and sets it as this cookie (login_submit in app.py).
    echo "AUTH=wytchr_token=$API_TOKEN"
    ;;
*)
    echo "usage: sh factory/visual.sh {steps|setup|serve|auth}" >&2
    exit 2
    ;;
esac
