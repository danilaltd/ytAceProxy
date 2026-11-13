# ytAceProxy

Short entry point for a new session. Read this first; details are linked below.

## What this project is

An `aiohttp` application that resolves streaming URLs with `yt-dlp` and returns
HTTP 302 redirects to the resolved link. Redirects are stored in SQLite and
managed through a server-rendered admin UI. The application is packaged as a
Docker image and deployed as two independent stacks:

| | prod | dev |
|---|---|---|
| deploy app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| release | `prod-*` | `dev-*` |
| compose/data | `/srv/ytaceproxy-prod`, `/var/lib/ytaceproxy-prod` | `/srv/ytaceproxy-dev`, `/var/lib/ytaceproxy-dev` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream / acexy | `6878` / `8090` | `6879` / `8091` |
| backup | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |

The image is `ghcr.io/danilaltd/ytaceproxy-proxy`. CI builds it on a
GitHub-hosted runner and deploys immutable digests; it never edits compose.
Ansible (separate repository `arch2debian`) owns compose, ports, volumes,
healthchecks, hooks and the SQLite backup.

## Repositories and layers

| Layer | Repository | Responsibility |
|---|---|---|
| application | `danilaltd/ytAceProxy` (this repo) | `proxy_ace/*`, Dockerfile, requirements/uv lock, admin templates |
| release pipeline | `.github/workflows/release.yml` | build, companion digests, manifest, Tailscale + forced-command deploy |
| server infrastructure | `danilaltd/arch2debian` | compose, exposure matrix, data dirs, pre/post hooks, wrapper allowlist, verify |
| server state | `/var/lib/deploy/<app>` | `manifest.json`, `manifest.prev.json`, log |

## Runtime shape

```text
client ──GET /yt_dlp/<name>──▶ manager :80 (aiohttp)
                                  │
                                  ├─ redirect_channels in proxies.db
                                  └─ yt-dlp subprocess (timeout 60 s)

client ──GET /ace/...──────────▶ acexy :80 ──▶ acestream engine :6878
                                  (companion images, not this service)
```

The manager, `acestream` and `acexy` are separate containers in the same
compose project. The manager does not stream video itself; it only resolves
links and redirects. The `/ace` redirect into `acexy` is planned, not current.

## Invariants

Read these before changing anything.

1. **Port 80 inside the manager container.** `LISTEN_PORT = 80` in
   `proxy_ace/app.py`; host ports are assigned by Ansible.
2. **SQLite at `/app/data/proxies.db`.** The path is in `proxy_ace/db.py`;
   the host directory is mounted by Ansible. Do not introduce a different
   database path without changing the role, hooks and backups.
3. **Schema is auto-created, not migrated.** `init_db()` runs
   `CREATE TABLE IF NOT EXISTS`. There is no migration framework. A schema
   change is a deploy-time risk; take a backup before deploying.
4. **`redirect_channels` is the active table.** `ace_streaming_channels` is
   legacy and must not gain new writes. See [docs/roadmap.md](docs/roadmap.md).
5. **Unknown channel falls back to `placeholder`.** If neither the channel nor
   `placeholder` has a `redirect_url`, `/yt_dlp/{name}` returns 404. Do not
   change this contract silently.
6. **URL resolution is out-of-process.** `yt-dlp` runs in a separate process
   with a 60-second timeout and a concurrency limit of 2. Never call
   `yt-dlp` inline in the event loop.
7. **Admin UI is tailnet-only.** There is no basic auth and no production
   approval. Do not add a public bind or an auth bypass.
8. **CI only changes image digests.** Compose, ports, volumes, hooks and
   backups are Ansible-managed. If a change needs a new file under `/srv` or
   `/etc`, change the Ansible role/template, not the host.
9. **Deploy references are immutable digests.** The forced command accepts
   only `ghcr.io/...@sha256:<64hex>` from the per-app allowlist. There is no
   tag fallback.
10. **Prod and dev are independent.** Separate directories, data, ports,
    manifests, logs and backups. Never point prod at the dev data directory.
11. **No secrets in git or CI.** `.env`, keys, tokens and `channels.db` are
    untracked. Runtime secrets, if added, go through Ansible Vault and `.env`.
12. **Do not restore the old `channels.db`.** It is a different JSON-era
    store; `proxies.db` is the SQLite database.

## Commands

```bash
# local (current pip flow)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python -m proxy_ace.app

# local (planned uv flow)
uv sync
uv run python -m proxy_ace.app

# image
docker build -t ytaceproxy-proxy .
docker run --rm -p 8089:80 -v "$PWD/data:/app/data" ytaceproxy-proxy

# CI status
gh run list --repo danilaltd/ytAceProxy --workflow Release
```

Server checks (run from the control host with access to `debian`):

```bash
APP=ytaceproxy-dev
ssh asus@debian "sudo jq . /var/lib/deploy/$APP/manifest.json"
ssh asus@debian "sudo tail -n 80 /var/log/deploy/$APP.log"
ssh asus@debian "sudo docker compose -p $APP -f /srv/$APP/docker-compose.yml ps"
```

## Where to look

| Question | File |
|---|---|
| endpoints, data model, runtime flow | [docs/architecture.md](docs/architecture.md) |
| how to deploy, environments, change matrix, rollback | [docs/deployment.md](docs/deployment.md) |
| local run, adding routes, uv migration | [docs/development.md](docs/development.md) |
| real failures and fixes | [docs/troubleshooting.md](docs/troubleshooting.md) |
| planned ACE cleanup / acexy redirect / uv | [docs/roadmap.md](docs/roadmap.md) |
| Ansible role, ports, hooks | `arch2debian/roles/ytaceproxy/README.md` |
| manifest contract and wrapper | `arch2debian/docs/ci-cd.md` |

## Changing what

| Change | Touch |
|---|---|
| redirect behaviour / new route | `proxy_ace/handler.py`, `proxy_ace/app.py`, tests, push `dev` |
| admin UI | `proxy_ace/admin_routes.py`, `proxy_ace/forms/*.html` |
| resolver format/timeout | `proxy_ace/config.py` |
| DB columns/tables | `proxy_ace/db.py`, `proxy_ace/repo.py`, `post-deploy.sh.j2`, backup |
| Python dependencies | `requirements.txt` now; `pyproject.toml` + `uv.lock` after uv migration |
| base image / runtime | `Dockerfile`; keep port 80 and `/app/data` |
| published port / bind | `arch2debian/inventory/group_vars/debian_servers/exposure.yml` |
| compose service, volume, healthcheck, logging | `arch2debian/roles/ytaceproxy/templates/docker-compose.yml.j2` |
| pre/post deploy hooks | `arch2debian/roles/ytaceproxy/templates/*.j2` |
| companion image versions | `.github/workflows/release.yml` env + Ansible role defaults |
| manifest keys / allowlist | workflow `assert`, `arch2debian/roles/deploy/templates/deploy-app-root.j2` |
| verify expectations | `arch2debian/roles/verify/defaults/main.yml` |

## Documentation rules

- Keep the README to one page; put details in `docs/<subject>.md`.
- Record invariants, ordering constraints and “do not touch” rules; skip code
  walkthroughs that can be read in a minute.
- A contract change (endpoint, env var, table, manifest key, port) updates the
  document that states it in the same commit.
- Do not put secrets, real tokens, hostnames or host paths in docs.
- When code and docs disagree, the code wins: fix or delete the stale text.

## Planned changes

The next agreed work is:

1. remove the remaining ACE artifacts (`ace_streaming_channels`, the
   `load_channels` stub) and add a convenient `/ace/{name}` redirect to
   `acexy`;
2. migrate to `uv` (`pyproject.toml`, `uv.lock`, Dockerfile, docs);
3. add tests and a small health endpoint.

Do not implement these silently as part of an unrelated change; use
[docs/roadmap.md](docs/roadmap.md).
