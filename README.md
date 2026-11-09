# Proxy ACE

An asynchronous `aiohttp` service that resolves streaming URLs with `yt-dlp`
and redirects clients to the direct playback link. The admin UI manages named
redirects; a SQLite database stores them.

## What it does

- `GET /yt_dlp/{channel}` — 302 redirect to the resolved URL for a channel.
  If the channel is unknown, it falls back to the `placeholder` redirect when
  one exists; otherwise it returns 404.
- `GET /yt_dlp/{channel}/upd` — re-resolves one channel on demand.
- `GET /admin/redirects` — admin UI: list, add, edit, delete and sync
  redirects. The UI also exposes a sync action.
- URL resolution runs in a separate process (`yt-dlp`) with a timeout, so a
  slow extraction does not block the HTTP server.

There is no `channels.json`, no file watcher, no watchdog, and no producer
loop anymore. The old `/ace/{channel}` streaming endpoint and the old
`channels.db` are not part of this version.

## Run locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m proxy_ace.app
```

The app listens on **container/local port 80** (`LISTEN_PORT` in
`proxy_ace/app.py`). The admin UI is at
<http://localhost:80/admin/redirects>.

Dependencies are pinned in `requirements.txt` (`aiohttp`, `yt-dlp`,
`aiosqlite`, `aiohttp-jinja2`, `WTForms`).

## Data

SQLite database: `/app/data/proxies.db` inside the container
(`proxy_ace/db.py`). The schema is created automatically on startup:

- `ace_streaming_channels`;
- `redirect_channels` (`name`, `url`, `redirect_url`, `ttl`, `created_at`).

Mount `/app/data` from the host to persist it. Do not copy the old
`channels.db` into this path; the format is different (JSON → SQLite).

## Docker

Build and run:

```bash
docker build -t ytaceproxy-proxy .
docker run --rm \
  -p 8089:80 \
  -v "$PWD/data:/app/data" \
  ytaceproxy-proxy
```

The image is published as
`ghcr.io/danilaltd/ytaceproxy-proxy` by the release workflow.

## CI/CD

`.github/workflows/release.yml` builds the image on a GitHub-hosted runner,
pushes it to GHCR tagged `<env>-<run_number>`, resolves the pinned companion
digests (`acestream-http-proxy`, `acexy`), and deploys a digest manifest over
the forced-command SSH wrapper. `dev` deploys the `dev-*` release, `main`
deploys `prod-*`. There is no self-hosted runner and no basic auth; the admin
UI is reachable only over the tailnet in production.

The Ansible repository owns compose, ports, volumes and the SQLite pre-deploy
backup. CI only changes image digests.
