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

## Current state (snapshot: 2026-11-14)

- Both stacks are deployed and healthy: `ytaceproxy-prod` release `prod-8`,
  `ytaceproxy-dev` release `dev-7`.
- `verify_apps: true`; the last `40_verify` passed.
- Documentation-only pushes (`README.md`, `AGENTS.md`, `docs/**`) skip the
  release workflow; workflow/code/dependency changes still deploy.
- Application documentation is complete:
  `README.md`, `AGENTS.md`, `docs/architecture.md`, `docs/deployment.md`,
  `docs/development.md`, `docs/troubleshooting.md`, `docs/roadmap.md`.
- Next agreed work is the ACE cleanup, the `/ace` → acexy redirect and the uv
  migration. Do them as separate changes, dev first.

The release numbers above are a snapshot, not an invariant. Check before
relying on them:

```bash
gh run list --repo danilaltd/ytAceProxy --workflow Release --limit 2
ssh asus@debian "sudo jq -r .release /var/lib/deploy/ytaceproxy-dev/manifest.json"
ssh asus@debian "sudo jq -r .release /var/lib/deploy/ytaceproxy-prod/manifest.json"
```

## Session start

1. `git status --short --branch` — the tree should be clean; for code changes
   you should be on `dev`, not `main`.
2. Read this file, then the document for the area you will touch
   ([docs/README.md](docs/README.md) is the index).
3. Check the latest release and manifest (commands above) before assuming a
   version.
4. Decide the change type with the “Changing what” table below: application
   code/CI, server shape (Ansible), or documentation only.
5. Update the document that states the changed contract **in the same commit**.
6. Follow the Git workflow below; never commit a code change directly to
   `main`.

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

## Git workflow

Work on `dev`; `main` is the production/release branch.

```bash
git checkout dev
git pull --ff-only origin dev
# edit, test, update docs
git add -A
git commit -m "type(scope): summary"
git push origin dev
# wait for the dev Release run unless the push is documentation-only
# verify dev
git checkout main
git merge --ff-only dev
git push origin main
```

Rules:

- **Never commit a code, dependency, Dockerfile or workflow change directly to
  `main`.** Push `dev`, wait for the dev deploy, verify it, then fast-forward
  `main`.
- Documentation-only changes (`README.md`, `AGENTS.md`, `docs/**`) are skipped
  by the release workflow. Still keep `dev` and `main` identical: commit on
  `dev`, then fast-forward `main`.
- If `main` has diverged (the merge is not a fast-forward), stop and report;
  do not create a merge commit or force-push.
- One concern per commit, message `type(scope): summary`. Do not rewrite
  history or move tags.
- A failed dev deploy blocks `main`. A failed prod deploy is rolled back by the
  wrapper; do not “fix” it by pushing another commit blindly.

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
- New documentation goes to an existing subject file when possible. If a new
  topic appears, create `docs/<topic>.md` and add it to
  [docs/README.md](docs/README.md); do not create another top-level `.md`.
- `AGENTS.md` holds the session map, invariants, Git workflow and current
  snapshot — not feature tutorials.
- Record invariants, ordering constraints and “do not touch” rules; skip code
  walkthroughs that can be read in a minute.
- A contract change (endpoint, env var, table, manifest key, port, CI
  behaviour) updates the document that states it in the same commit.
- Planned work goes to [docs/roadmap.md](docs/roadmap.md); completed work
  removes the stale roadmap item.
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
