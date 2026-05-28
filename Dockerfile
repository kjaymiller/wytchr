FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv pip install --system --no-cache \
    "quart==0.20.*" \
    "hypercorn==0.17.*" \
    "apscheduler==3.*" \
    "httpx==0.28.*" \
    "psycopg[binary]==3.*"

WORKDIR /app
COPY *.py /app/
COPY templates /app/templates

EXPOSE 5000
# Hypercorn ASGI server. Single worker is plenty for the single-operator
# load; the event loop fans concurrent requests + the AsyncIOScheduler
# poll job across one process. --access-logfile - sends access logs to
# stdout so `docker compose logs` shows them.
CMD ["hypercorn", "app:app", "--bind", "0.0.0.0:5000", "--workers", "1", "--access-logfile", "-"]
