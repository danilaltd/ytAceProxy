# ytAceProxy

Asynchronous `aiohttp` service that resolves streaming URLs with `yt-dlp` and
redirects clients to the direct playback URL. It also maps named ACE channels to
the per-environment `acexy` stream endpoint. Named redirects and ACE channels
live in SQLite and are managed through a small server-rendered admin UI.

The application is deployed as two independent stacks, `ytaceproxy-prod` and
`ytaceproxy-dev`. Ansible owns `.env` values (ports, binds, data dirs), hooks
and backups; CI builds the image and ships the compose shape with the digest
manifest.

- Architecture and request flow: [docs/architecture.md](docs/architecture.md)
- Deployment, environments and change matrix: [docs/deployment.md](docs/deployment.md)
- Local development, tests and uv: [docs/development.md](docs/development.md)
- Failures seen in practice: [docs/troubleshooting.md](docs/troubleshooting.md)
- Planned work (tests, health, optional acestream exposure): [docs/roadmap.md](docs/roadmap.md)
- Agent entry point: [AGENTS.md](AGENTS.md)

## Current state

Snapshot (2026-11-19): `ytaceproxy-dev` is `dev-11` and `ytaceproxy-prod` is
`prod-12`. Both run manifest v2 and compose
`sha256 c5d9ed96…`; `ace_channels` exists on both, the legacy
`ace_streaming_channels` was dropped on prod after a backup, and `/ace`
redirects through acexy on both. The post-deploy hook still accepts either
table during the transition. Release numbers are a snapshot, not an invariant
— check `/var/lib/deploy/<app>/manifest.json` or the latest `Release` run.
Session start, Git rules and live-state commands are in
[AGENTS.md](AGENTS.md); ownership is in
`arch2debian/docs/deployment-contract.md`.

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| GET | `/yt_dlp/{channel}` | 302 to the stored `redirect_url`; falls back to the `placeholder` redirect; otherwise 404 |
| GET | `/yt_dlp/{channel}/upd` | re-resolves one channel now; returns `200 text/plain` |
| GET | `/ace/{channel}` | 302 to `<ACEXY_BASE_URL>/ace/getstream?id=<content_id>`; unknown channel is 404; missing `ACEXY_BASE_URL` is 503 |
| GET | `/health` | 200 `ok` when SQLite and `redirect_channels` are readable; 503 otherwise; used by the manager healthcheck |
| GET | `/admin/redirects` | admin list of yt-dlp redirects: add, edit, delete, sync |
| GET/POST | `/admin/redirects/add` | create a redirect |
| GET/POST | `/admin/redirects/edit/{id}` | edit a redirect |
| GET | `/admin/redirects/delete/{id}` | delete a redirect |
| GET | `/admin/redirects/sync` | re-resolve all redirects in the background |
| GET | `/admin/ace` | admin list of ACE channels: add, edit, delete |
| GET/POST | `/admin/ace/add` | create an ACE channel |
| GET/POST | `/admin/ace/edit/{id}` | edit an ACE channel |
| GET | `/admin/ace/delete/{id}` | delete an ACE channel |

`/ace` names are independent from `/yt_dlp` names: the same name can have a
yt-dlp redirect and an ACE content id. The manager never proxies video; it
returns a 302 and the media connection is between the client and
`acexy`/`acestream`.

## Data

SQLite file: `/app/data/proxies.db` (container path). The schema is created on
startup from `proxy_ace/db.py`.

`redirect_channels` is the yt-dlp table:

| Column | Meaning |
|---|---|
| `id` | primary key |
| `name` | unique channel name used in `/yt_dlp/{name}` |
| `url` | source URL passed to `yt-dlp` |
| `redirect_url` | resolved URL returned as a 302 |
| `ttl` | expiry timestamp extracted from the resolved URL; `-1` means eternal |
| `created_at` | creation timestamp |

`ace_channels` is the ACE table:

| Column | Meaning |
|---|---|
| `id` | primary key |
| `name` | unique channel name used in `/ace/{name}` |
| `content_id` | 40-character AceStream content id/hash |
| `created_at` | creation timestamp |

A fresh database contains only these two tables. The old
`ace_streaming_channels` table is no longer created and was dropped on dev and
prod (prod backup:
`/var/backups/ytaceproxy/prod/pre-2026-11-19-133555.db`). The post-deploy hook
still accepts it during the transition; see
[docs/roadmap.md](docs/roadmap.md).

## Run locally

Dependency management is `uv` (`pyproject.toml` + `uv.lock`):

```bash
uv sync --frozen
ACEXY_BASE_URL=http://localhost:8090 uv run python -m proxy_ace.app
uv run pytest
```

The app listens on port **80** (`LISTEN_PORT` in `proxy_ace/app.py`), so either
run with the needed privileges or temporarily change `LISTEN_PORT` on a local
branch. Open <http://localhost/admin/redirects> or
<http://localhost/admin/ace>.

The database path is the container path `/app/data/proxies.db`. For a
non-Docker run, mount or create that directory, or change `DB_PATH` in a local
patch. Do not commit a local path change.

The local stack in `docker-compose.yml` sets `ACEXY_BASE_URL` to
`http://localhost:8090` by default; override `ACEXY_BASE_URL` when the browser
is not on the Docker host.

## Docker

Build and run the application image:

```bash
docker build -t ytaceproxy-proxy .
docker run --rm \
  -p 8089:80 \
  -e ACEXY_BASE_URL=http://localhost:8090 \
  -v "$PWD/data:/app/data" \
  ytaceproxy-proxy
```

The published image is `ghcr.io/danilaltd/ytaceproxy-proxy`. The root
`docker-compose.yml` is a **local development stack only** (`manager`,
`acestream`, `acexy`, host ports 8088/6878/8090). The server stack shape is
`deploy/compose/docker-compose.yml`; the release embeds it in manifest v2 with
its sha256 and `required_env`. Ansible writes `.env`, hooks and `compose.sh`;
the wrapper writes `/srv/<app>/docker-compose.yml` and `deploy-images.env`.

## Deploy

- `dev` push → image built and deployed as `ytaceproxy-dev` (`dev-<run>`).
- `main` push → image built and deployed as `ytaceproxy-prod` (`prod-<run>`).
- The workflow builds on GitHub-hosted runners, resolves companion digests,
  validates the server compose with `deploy/compose/ci.env`, calls
  `deploy/build_manifest.py` to build manifest v2
  (`app`, `config`, `images`, `release`), joins the tailnet and calls the
  forced-command `deploy-app <app> <release> <base64url>` over SSH.
- Ansible owns `.env` values (ports, binds, `DATA_DIR`, `ACEXY_BASE_URL`),
  hooks, `compose.sh` and backups; the release owns the compose shape, its
  hash and the image digests.
- A compose-only change is a release with the same digests and a new hash.
- Admin UI is tailnet-only; there is no basic auth and no production approval.

The full flow, command reference and "what to change when" table are in
[docs/deployment.md](docs/deployment.md).

## Planned changes

1. Add structured logging improvements (the `/health` endpoint is done).
2. Remove the host-published `acestream` ports after all clients use `/ace`
   through `acexy`.
3. Revisit memory limits after a week of `docker stats` data.

The concrete checklists are in [docs/roadmap.md](docs/roadmap.md).
