# wytchr

Manual-download UI on top of [ytdl-sub-api](https://github.com/kjaymiller/ytdl-sub-api).

Polls subscribed YouTube channels for recent uploads, surfaces them in a
kanban-by-profile board, and downloads only the videos you click. Tags +
profile-driven display windows turn the board into a curation tool rather than
a download log.

Flask + HTMX + SortableJS + SQLite.

## Run

```sh
docker run --rm -p 5000:5000 \
  -e API_TOKEN=... \
  -e YTDL_SUB_API_URL=http://ytdl-sub-api:5000 \
  -e YTDL_SUB_API_TOKEN=... \
  -v wytchr-data:/data \
  ghcr.io/kjaymiller/wytchr:latest
```

## Environment

| Var | Default | Notes |
| --- | --- | --- |
| `API_TOKEN` | — | required; gates wytchr's own endpoints |
| `YTDL_SUB_API_URL` | `http://ytdl-sub-api:5000` | upstream ytdl-sub-api base |
| `YTDL_SUB_API_TOKEN` | `$API_TOKEN` | bearer for ytdl-sub-api |
| `DB_PATH` | `/data/wytchr.db` | SQLite path |
| `POLL_INTERVAL_MINUTES` | `30` | channel poll cadence |
| `POLL_LIMIT` | `30` | entries per channel per poll |

## Image

Published to `ghcr.io/kjaymiller/wytchr` on every push to `main` and on tags.
