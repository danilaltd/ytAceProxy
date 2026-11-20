# playgate

Asynchronous `aiohttp` service that resolves stream source URLs with `yt-dlp`
and redirects clients (HTTP 302) to the direct playback URL. Channels live in
SQLite and are managed through a small server-rendered admin UI.

The code-level name is **playgate**. The GitHub repository (`ytAceProxy`), the
image (`ghcr.io/danilaltd/ytaceproxy-proxy`) and the deploy apps
(`ytaceproxy-prod` / `ytaceproxy-dev`) keep their historical names until the
deferred infrastructure rename; see [docs/roadmap.md](docs/roadmap.md).

The application is deployed as two independent stacks, `ytaceproxy-prod` and
`ytaceproxy-dev`. Ansible owns `.env` values (ports, binds, data dirs), hooks
and backups; CI builds the image and ships the compose shape with the digest
manifest.

- Architecture and request flow: [docs/architecture.md](docs/architecture.md)
- Deployment, environments and change matrix: [docs/deployment.md](docs/deployment.md)
- Local development, tests and uv: [docs/development.md](docs/development.md)
- Failures seen in practice: [docs/troubleshooting.md](docs/troubleshooting.md)
- Planned work (logging, companions decision, memory limits): [docs/roadmap.md](docs/roadmap.md)
- Agent entry point: [AGENTS.md](AGENTS.md)

## Current state

Snapshot (2026-11-20): `ytaceproxy-dev` is `dev-21` and `ytaceproxy-prod` is
`prod-22`; both run manifest v2 and compose `sha256 3dc0d3fd…`. The
single-table `/play` model is deployed on both stacks. Release numbers are a
snapshot, not an invariant — check `/var/lib/deploy/<app>/manifest.json` or the latest `Release`
run. Session start, Git rules and live-state commands are in
[AGENTS.md](AGENTS.md); ownership is in
`arch2debian/docs/deployment-contract.md`.

## Endpoints

| Method | Path | Behaviour |
|---|---|---|
| GET | `/play/{channel}` | 302 to the stored `redirect_url`; falls back to the `placeholder` channel; otherwise 404 |
| GET | `/play/{channel}/upd` | re-resolves one channel now; returns `200 text/plain` |
| GET | `/health` | 200 `ok` when SQLite and `channels` are readable; 503 otherwise; used by the manager healthcheck |
| GET | `/admin` | redirects to `/admin/channels` |
| GET | `/admin/channels` | admin list: add, edit, delete, update now, sync all |
| GET/POST | `/admin/channels/add` | create a channel |
| GET/POST | `/admin/channels/{id}/edit` | edit a channel |
| POST | `/admin/channels/{id}/delete` | delete a channel |
| POST | `/admin/channels/{id}/update` | re-resolve one channel in the background |
| POST | `/admin/channels/sync` | re-resolve all channels in the background |

The manager never proxies video: it returns a 302 and the media connection is
between the client and the origin server. `/ace` and the ACE content-id storage
were removed; the `acestream`/`acexy` companion containers still deploy but are
not referenced by the application (their fate is an open roadmap decision).

## Data

SQLite file: `/app/data/channels.db` (container path). The schema is created on
startup from `playgate/db.py`.

`channels` is the only table:

| Column | Meaning |
|---|---|
| `id` | primary key |
| `name` | unique channel name used in `/play/{name}` |
| `url` | source URL passed to `yt-dlp` |
| `format` | optional per-channel yt-dlp format selector; empty means `best[acodec!=none]` |
| `redirect_url` | resolved URL returned as a 302 (or a manual override) |
| `ttl` | expiry timestamp extracted from the resolved URL; `-1` means eternal; `NULL` unknown |
| `created_at` | creation timestamp |

There are no migrations. A fresh database gets exactly this table; an old
`proxies.db` is obsolete and was removed during the single-table rollout.

## Run locally

Dependency management is `uv` (`pyproject.toml` + `uv.lock`):

```bash
uv sync --frozen
uv run python -m playgate.app
uv run pytest --cov=playgate
uv run pre-commit run --all-files
```

Quality gates: ruff (lint+format), ty, deptry, pytest with an 80% coverage
gate, and pre-commit hooks; CI runs the same checks before building.

The app listens on port **80** (`LISTEN_PORT` in `playgate/app.py`), so either
run with the needed privileges or temporarily change `LISTEN_PORT` on a local
branch. Open <http://localhost/admin/channels>.

The database path is the container path `/app/data/channels.db`. For a
non-Docker run, mount or create that directory, or change `DB_PATH` in a local
patch. Do not commit a local path change.

The root `docker-compose.yml` is a local development stack (`manager`,
`acestream`, `acexy`). The manager has no ACE configuration anymore; companions
are kept only for local parity with the server.

## Docker

Build and run the application image:

```bash
docker build -t ytaceproxy-proxy .
docker run --rm \
  -p 8089:80 \
  -v "$PWD/data:/app/data" \
  ytaceproxy-proxy
```

The published image is `ghcr.io/danilaltd/ytaceproxy-proxy`. The server stack
shape is `deploy/compose/docker-compose.yml`; the release embeds it in manifest
v2 with its sha256 and `required_env`. Ansible writes `.env`, hooks and
`compose.sh`; the wrapper writes `/srv/<app>/docker-compose.yml` and
`deploy-images.env`.

## Deploy

- `dev` push → image built and deployed as `ytaceproxy-dev` (`dev-<run>`).
- `main` push → image built and deployed as `ytaceproxy-prod` (`prod-<run>`).
- The workflow builds on GitHub-hosted runners, resolves companion digests,
  validates the server compose with `deploy/compose/ci.env`, calls
  `deploy/build_manifest.py` to build manifest v2
  (`app`, `config`, `images`, `release`), joins the tailnet and calls the
  forced-command `deploy-app <app> <release> <base64url>` over SSH.
- Ansible owns `.env` values (ports, binds, `DATA_DIR`), hooks, `compose.sh` and
  backups; the release owns the compose shape, its hash and the image digests.
- A compose-only change is a release with the same digests and a new hash.
- Admin UI is tailnet-only; there is no basic auth and no production approval.

The full flow, command reference and "what to change when" table are in
[docs/deployment.md](docs/deployment.md).

## Planned changes

1. Structured logging improvements (request/channel fields).
2. Decide the fate of the `acestream`/`acexy` companions: host-level Docker or
   removal (deferred, not decided).
3. Revisit memory limits after a week of `docker stats` data.

The concrete checklists are in [docs/roadmap.md](docs/roadmap.md).
