# ytAceProxy (code name: playgate)

Short entry point for a new session. Read this first; details are linked below.

## What this project is

An `aiohttp` application that resolves a stream source URL with `yt-dlp` and
returns an HTTP 302 to the direct playback URL. Channels live in SQLite and are
managed through a server-rendered admin UI. The application is packaged as a
Docker image and deployed as two independent stacks:

| | prod | dev |
|---|---|---|
| deploy app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| release | `prod-*` | `dev-*` |
| compose/data | `/srv/ytaceproxy-prod`, `/var/lib/ytaceproxy-prod` | `/srv/ytaceproxy-dev`, `/var/lib/ytaceproxy-dev` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream / acexy (unused by the app) | `6878` / `8090` | `6879` / `8091` |
| backup | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |

**Naming (scope decided 2026-11-20):** the code-level project is `playgate`
(`playgate/*`, `/play/{name}`, `/admin/channels`, `/app/data/channels.db`). The
GitHub repository (`ytAceProxy`), image (`ghcr.io/danilaltd/ytaceproxy-proxy`),
deploy apps (`ytaceproxy-*`), `/srv` paths, Ansible role and exposure keys keep
their historical names until the deferred infrastructure rename. Do not rename
those piecemeal.

The image is `ghcr.io/danilaltd/ytaceproxy-proxy`. CI builds it on a
GitHub-hosted runner and deploys immutable digests plus the compose shape
(`deploy/compose/docker-compose.yml`). Ansible (separate repository
`arch2debian`) owns `.env` values (ports, binds, data dirs), hooks, `compose.sh`
and the SQLite backup. It does not render compose.

## Deployment model

Canonical contract: `arch2debian/docs/deployment-contract.md`.

| Layer | Owner | Contents |
|---|---|---|
| host | `arch2debian` | OS, SSH, network, firewall, Docker |
| host services | `arch2debian` (`30_services`) | wrapper, compose policy, nginx, AdGuard, CUPS |
| environment | `arch2debian` (`31_apps -e env=…`) | dirs, `.env`, hooks, `compose.sh`, `env-pins.json` |
| stack shape | this repository (`deploy/compose/docker-compose.yml`, `env.required`, `ci.env`) | compose and the `.env` contract |
| release/apply | this repository's release workflow + server wrapper | manifest v2, compose hash, apply/rollback |

- Ansible never writes application compose and never starts application
  containers.
  `/srv/<app>/docker-compose.yml` is written only by the wrapper;
  `/srv/<app>/{.env,compose.sh,pre-deploy.sh,post-deploy.sh}` are
  Ansible-managed.
- Manifest v2 is exactly `{app, release, images, config}`;
  `config = {compose_b64, compose_sha256, required_env}`. Images are digests
  only; there is no tag fallback.
- The wrapper checks the schema, allowlist, compose hash, `.env` (root 0600,
  every required name non-empty) and `/usr/local/libexec/deploy-compose-policy`
  before applying. The guards reject host escapes/build, binds outside
  `DATA_DIR`, ports outside `arch2debian`'s exposure matrix, mutable images and
  non-`json-file` logging.
- A compose change is a release with the same digests and a new hash.
- Commands: `31_apps -e env=dev|prod` for env/hooks, a release for compose,
  `40_verify` for the contract check.

## Current state (snapshot: 2026-11-20)

- Both stacks are healthy: `ytaceproxy-prod` release `prod-20` and
  `ytaceproxy-dev` release `dev-19`, both compose `sha256 3dc0d3fd…`.
- The single-table `/play` model is deployed on both stacks; prod has 2
  channels, dev has 0. The old `proxies.db` was removed on both.
- `/ace`, `ace_channels`, `ACEXY_BASE_URL` and the ACE admin pages are removed.
  The `acestream`/`acexy` containers still deploy but nothing in the app uses
  them; their fate is a deferred roadmap decision.
- The SQLite file is `/app/data/channels.db` with one table, `channels`. The old
  `proxies.db` is obsolete and is deleted per environment after the rollout.
- `verify_apps: true`; the last `40_verify` passed.
- Documentation-only pushes (`README.md`, `AGENTS.md`, `docs/**`) skip the
  release workflow; workflow/code/dependency changes still deploy.
- Application documentation is complete: `README.md`, `AGENTS.md`,
  `docs/architecture.md`, `docs/deployment.md`, `docs/development.md`,
  `docs/troubleshooting.md`, `docs/roadmap.md`.
- Next agreed work: structured logging, the companions decision, memory
  limits.

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
| application | `danilaltd/ytAceProxy` (this repo) | `playgate/*`, tests, Dockerfile, `pyproject.toml`/`uv.lock`, admin templates |
| server shape | `danilaltd/ytAceProxy` (`deploy/compose/`) | `docker-compose.yml`, `env.required`, `ci.env` |
| release pipeline | `.github/workflows/release.yml` | build, companion digests, manifest v2, Tailscale + forced-command deploy |
| server infrastructure | `danilaltd/arch2debian` | `.env`, exposure matrix, data dirs, hooks, `compose.sh`, wrapper/policy, verify |
| server state | `/var/lib/deploy/<app>` | `manifest.json`, `manifest.prev.json`, `compose.prev.yml`, log |

## Runtime shape

```text
client ──GET /play/<name>───▶ manager :80 (aiohttp)
                                  │
                                  ├─ channels in /app/data/channels.db
                                  └─ yt-dlp subprocess (timeout 60 s)
                                  │
                                  └─ 302 Location ──▶ client
```

The deployed compose still contains `acestream` and `acexy`, but the manager
does not reference them. The manager never streams video: it only resolves a
URL and redirects.

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
   `playgate/app.py`; host ports are assigned by Ansible.
2. **SQLite at `/app/data/channels.db`.** The path is in `playgate/db.py`; the
   host directory is mounted by Ansible. The pre/post hooks reference the same
   file. Do not introduce a different database path without changing the role,
   hooks and backups.
3. **Schema is auto-created, not migrated.** `init_db()` runs
   `CREATE TABLE IF NOT EXISTS`. There is no migration framework. A schema
   change is a deploy-time risk; take a backup before deploying.
4. **`channels` is the only table.** ACE tables and content ids are gone. Do
   not reintroduce `/ace`, `ace_channels` or `ACEXY_BASE_URL` silently.
5. **Unknown channel falls back to `placeholder`.** If neither the channel nor
   `placeholder` has a `redirect_url`, `/play/{name}` returns 404. Do not
   change this contract silently.
6. **URL resolution is out-of-process.** `yt-dlp` runs in a separate process
   with a 60-second timeout and a concurrency limit of 2. Never call
   `yt-dlp` inline in the event loop.
7. **Per-channel quality uses `channels.format`.** Dedup is by
   `(source URL, format)`. Merge selectors (`+`) are rejected: the image has no
   ffmpeg.
8. **Admin UI is tailnet-only.** There is no basic auth and no production
   approval. Do not add a public bind or an auth bypass.
9. **CI ships image digests and the compose shape.** The server compose is
   `deploy/compose/docker-compose.yml`, embedded in manifest v2 with its
   sha256 and `required_env`. `.env` values (ports, binds, `DATA_DIR`), hooks,
   `compose.sh` and backups remain Ansible-managed. `30_services` never writes
   compose; the wrapper writes `/srv/<app>`. Never edit `/srv` by hand.
10. **Deploy references are immutable digests.** The forced command accepts
    only `ghcr.io/...@sha256:<64hex>` from the per-app allowlist. There is no
    tag fallback.
11. **Prod and dev are independent.** Separate directories, data, ports,
    manifests, logs and backups. Never point prod at the dev data directory.
12. **No secrets in git or CI.** `.env`, keys, tokens and old `channels.db`
    copies are untracked. Runtime secrets, if added, go through Ansible Vault
    and `.env`.
13. **Do not restore the old `proxies.db` or `channels.db`.** Both are from
    older designs (`redirect_channels`/`ace_channels` and the JSON-era store);
    the current database is `/app/data/channels.db` with the `channels` table.
14. **Logs never contain source or resolved URLs.** They can carry tokens. Log
    channel names, TTL and sanitised errors only.

## Commands

```bash
# local
uv sync --frozen
uv run python -m playgate.app
uv run pytest

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
ssh asus@debian "sudo /srv/$APP/compose.sh ps"
# env/hooks apply
cd arch2debian
.venv/bin/ansible-playbook playbooks/31_apps.yml -e env=dev
# full contract check
.venv/bin/ansible-playbook playbooks/40_verify.yml
```

## Where to look

| Question | File |
|---|---|
| endpoints, data model, runtime flow | [docs/architecture.md](docs/architecture.md) |
| how to deploy, environments, change matrix, rollback | [docs/deployment.md](docs/deployment.md) |
| local run, adding routes, tests, uv | [docs/development.md](docs/development.md) |
| real failures and fixes | [docs/troubleshooting.md](docs/troubleshooting.md) |
| planned logging/companions/memory work | [docs/roadmap.md](docs/roadmap.md) |
| Ansible role, ports, hooks | `arch2debian/roles/ytaceproxy/README.md` |
| canonical ownership and manifest | `arch2debian/docs/deployment-contract.md` |
| CI/CD and wrapper | `arch2debian/docs/ci-cd.md` |

## Changing what

| Change | Touch |
|---|---|
| redirect behaviour / new route | `playgate/handler.py`, `playgate/app.py`, `tests/`, push `dev` |
| channel schema / queries | `playgate/db.py`, `playgate/repo.py`, `playgate/models.py`, `post-deploy.sh.j2`, backup |
| resolver format/timeout/concurrency | `playgate/config.py` |
| quality selector / form validation | `playgate/forms.py` |
| TTL badge | `playgate/ttl.py` |
| admin UI | `playgate/admin_routes.py`, `playgate/forms/*.html`, `playgate/static/admin.css` |
| Python dependencies | `pyproject.toml` + `uv.lock`; never edit `uv.lock` by hand |
| base image / runtime | `Dockerfile`; keep port 80 and `/app/data` |
| published port / bind | `arch2debian/inventory/group_vars/debian_servers/exposure.yml`, then `15_firewall`/`30_services`/`31_apps` |
| compose service, volume, healthcheck, logging | `deploy/compose/docker-compose.yml` (release) |
| required `.env` names / compose CI fixture | `deploy/compose/env.required`, `deploy/compose/ci.env` |
| pre/post deploy hooks | `arch2debian/roles/ytaceproxy/templates/*.j2`, applied by `31_apps` |
| companion image versions | `.github/workflows/release.yml` env |
| manifest keys / allowlist | `deploy/build_manifest.py`, `arch2debian/roles/deploy/templates/deploy-app-root.j2` |
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
- A contract change (endpoint, env var, table, manifest key, compose, port, CI
  behaviour) updates the document that states it in the same commit.
- Planned work goes to [docs/roadmap.md](docs/roadmap.md); completed work
  removes the stale roadmap item.
- Do not put secrets, real tokens, hostnames or host paths in docs.
- When code and docs disagree, the code wins: fix or delete the stale text.

## Planned changes

The next agreed work is:

1. structured logging with request/channel fields (URLs stay out of logs);
2. decide the fate of the `acestream`/`acexy` companions: host-level Docker or
   removal (deferred, not decided);
3. revisit memory limits after a week of `docker stats` data.

Do not implement these silently as part of an unrelated change; use
[docs/roadmap.md](docs/roadmap.md).
