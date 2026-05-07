FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN uv pip install --system --no-cache \
    "flask==3.*" \
    "apscheduler==3.*" \
    "httpx==0.28.*" \
    "yt-dlp==2026.3.17"

WORKDIR /app
COPY app.py /app/app.py
COPY templates /app/templates

ENV FLASK_APP=app.py \
    FLASK_RUN_HOST=0.0.0.0 \
    FLASK_RUN_PORT=5000

EXPOSE 5000
CMD ["python", "/app/app.py"]
