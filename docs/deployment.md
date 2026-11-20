# Deployment

This document answers four questions:

1. what is deployed and where;
2. how a change travels from a commit to the server;
3. which file to edit for each kind of change;
4. how to verify and roll back.

The canonical ownership contract is
`arch2debian/docs/deployment-contract.md`. This repository owns the image, the
server compose and the release; Ansible owns the host, `.env`, hooks and
`compose.sh`.

## 1. Layers and source of truth

| Layer | Source of truth | What it changes |
|---|---|---|
| application code | this repository, `playgate/*` | routes, redirect logic, resolver, admin UI |
| image | `Dockerfile` + dependencies | Python base, installed packages, entry point |
| server compose | `deploy/compose/docker-compose.yml` | services, volumes, healthcheck, logging |
| `.env` contract | `deploy/compose/env.required` | names the host `.env` must provide |
| release pipeline | `.github/workflows/release.yml`, `deploy/build_manifest.py` | build, companion digests, manifest v2, SSH deploy |
| host values | `arch2debian` (`31_apps`) | `.env`, data dirs, hooks, `compose.sh` |
| host ports | `arch2debian/inventory/group_vars/debian_servers/exposure.yml` | `ytace_prod_*` / `ytace_dev_*` |
| deploy wrapper/policy | `arch2debian/roles/deploy` | allowlist, compose hash, policy, apply/rollback |
| verification | `arch2debian/roles/verify` | expected containers, ports, contract |

The server runs wrapper v2: it accepts only a manifest with `config`
(`compose_b64`, `compose_sha256`, `required_env`) and rejects a v1 manifest.

Ownership of server files:

| Path | Owner |
|---|---|
| `/srv/ytaceproxy-<env>/docker-compose.yml` | deploy repository, written by the wrapper |
| `/srv/ytaceproxy-<env>/.env` | `31_apps` (0600) |
| `/srv/ytaceproxy-<env>/deploy-images.env` | wrapper (0600) |
| `/srv/ytaceproxy-<env>/compose.sh`, `pre-deploy.sh`, `post-deploy.sh` | `31_apps` |
| `/var/lib/deploy/<app>/manifest*.json`, `compose.prev.yml` | wrapper |

Do not edit `/srv` by hand. `30_services` never writes application compose and
never starts application containers; `31_apps` never writes compose.

## 2. Environments

| | prod | dev |
|---|---|---|
| deploy app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| release | `prod-<run>` | `dev-<run>` |
| directory | `/srv/ytaceproxy-prod` | `/srv/ytaceproxy-dev` |
| data | `/var/lib/ytaceproxy-prod` | `/var/lib/ytaceproxy-dev` |
| SQLite | `.../manager-data/channels.db` | `.../manager-data/channels.db` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream (unused by the app) | `6878` | `6879` |
| acexy (unused by the app) | `8090` | `8091` |
| backup | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |
| manifest/log | `/var/lib/deploy/<app>`, `/var/log/deploy/<app>.log` | same pattern |

The two stacks share no database, cache, port or manifest and can run at the
same time. Current releases are `prod-22` and `dev-21`, both compose
`sha256 3dc0d3fd…`; the live value is in each manifest.

## 3. Release flow

A push to `dev` or `main` triggers `.github/workflows/release.yml`.

```text
push to dev/main
  └─ security-scan (Gitleaks, full history)
  └─ test (uv sync --frozen + pytest with 80% coverage; gates the build)
  └─ lint (pre-commit: ruff/ty/deptry/hygiene + actionlint; gates the build)
  └─ build (GitHub-hosted runner)
       └─ docker compose config -q (deploy/compose, ci.env)
       └─ docker build, push ghcr.io/danilaltd/ytaceproxy-proxy:<env>-<run>
       └─ artifact: manager <repo>@sha256:<digest>
  └─ companions
       └─ resolve pinned acestream and acexy tags to digests
  └─ deploy
       ├─ deploy/build_manifest.py → manifest v2
       │    {app, release, images, config}
       │    config = {compose_b64, compose_sha256, required_env}
       ├─ base64url-encode manifest
       ├─ join tailnet (OAuth, tag:ci, ping debian)
       └─ ssh deploy@debian "ytaceproxy-<env> <release> <manifest>"
            └─ deploy-app (forced command, argument validation)
                 └─ sudo deploy-app-root
                      ├─ allowlist: app, release prefix, exactly 3 digest refs
                      ├─ validate config: compose size/hash, required_env
                      ├─ atomic manifest.json, save manifest.prev.json
                      ├─ pre-deploy.sh (consistent SQLite backup)
                      ├─ write deploy-images.env
                      ├─ resolve compose + deploy-compose-policy
                      ├─ save previous compose, replace compose atomically
                      ├─ docker compose pull
                      ├─ docker compose up -d --remove-orphans --wait
                      ├─ post-deploy.sh (integrity, tables, redirect count)
                      └─ on failure: previous manifest images + compose
```

Details:

- `main` → `ytaceproxy-prod` / `prod-*`; `dev` → `ytaceproxy-dev` / `dev-*`.
- No production approval; a merge to `main` deploys immediately.
- The `test` job runs `uv sync --frozen` and `uv run pytest -q
  --cov=playgate` (80% gate); the `lint` job runs `pre-commit run --all-files`
  (ruff, ty, deptry, hygiene) and actionlint. Both gate the build and deploy.
- The deploy job has no shell on the server beyond the forced command.
- Images are always `ghcr.io/...@sha256:<64 lowercase hex>`; a tag is never
  sent to the server.
- `pre-deploy.sh` is skipped only when the manager is not running or the
  database does not exist yet.
- `post-deploy.sh` failure rolls back the **image and compose**; the database
  is not restored automatically. Use the pre-deploy backup for that.
- A compose-only change is a normal release: same digests, new compose hash.
- `deploy/compose/ci.env` is only a CI fixture; never use it on a server.
- The workflow's `paths-ignore` covers `README.md`, `AGENTS.md` and
  `docs/**`, so documentation-only pushes do not build or deploy. A change to
  the workflow itself still triggers a release.

## 4. First install and manual commands

On a fresh host, Ansible creates the environment but not the stack:

```bash
cd arch2debian
.venv/bin/ansible-playbook playbooks/30_services.yml -e ansible_host=debian
.venv/bin/ansible-playbook playbooks/31_apps.yml -e env=all -e ansible_host=debian
```

`30_services` installs the wrapper and policy; `31_apps` creates directories,
`.env`, hooks and `compose.sh`, and records `/var/lib/deploy/env-pins.json`.
There is no bootstrap compose: the first release writes compose and starts the
containers. A stack without a manifest has no `/srv/<app>/docker-compose.yml`.

Useful commands on the control host:

```bash
APP=ytaceproxy-dev
DIR=/srv/$APP

ssh asus@debian "sudo jq . /var/lib/deploy/$APP/manifest.json"
ssh asus@debian "sudo jq -r .release /var/lib/deploy/$APP/manifest.prev.json"
ssh asus@debian "sudo tail -n 100 /var/log/deploy/$APP.log"
ssh asus@debian "sudo $DIR/compose.sh ps"
ssh asus@debian "sudo docker inspect -f '{{.Config.Image}}' $APP-manager-1"
ssh asus@debian "sudo cat $DIR/deploy-images.env"
```

Manual hook test:

```bash
ssh asus@debian "sudo $DIR/pre-deploy.sh manual-test"
ssh asus@debian "sudo ls -l /var/backups/ytaceproxy/<env>/"
```

## 5. What to change when

### 5.1 Application behaviour

| Change | Edit | Then |
|---|---|---|
| redirect/fallback logic, new route | `playgate/handler.py`, `playgate/app.py` | test locally, push `dev` |
| resolver format/timeout/concurrency | `playgate/config.py` | push `dev`; check `/play/.../upd` |
| quality selector / channel form | `playgate/forms.py` | push `dev`; verify UI |
| TTL badge | `playgate/ttl.py` | push `dev`; verify UI |
| admin routes/templates | `playgate/admin_routes.py`, `playgate/forms/*.html`, `playgate/static/admin.css` | push `dev`; verify UI |
| DB queries | `playgate/repo.py` | push `dev`; check post-deploy log |
| dependencies | `pyproject.toml` + `uv.lock` | `uv lock`, rebuild image |
| base image, port, entry point | `Dockerfile` | keep port 80 and `/app/data`; push `dev` |

No Ansible change is needed for normal application code: the image digest is
the only server-side change.

### 5.2 Database shape

The schema is created by `playgate/db.py` with `CREATE TABLE IF NOT EXISTS`;
there is no migration framework.

| Change | Edit | Extra |
|---|---|---|
| add a table | `playgate/db.py`, `playgate/repo.py` | created automatically on startup, including existing DBs |
| add a column | `playgate/db.py`, `playgate/repo.py` | existing DBs need a manual `ALTER TABLE` |
| required table for post-deploy | `arch2debian/roles/ytaceproxy/templates/post-deploy.sh.j2` | run `31_apps` |
| rename the database file | `playgate/db.py`, both hook templates, docs | wipe the old file after the release; see below |

The single-table `/play` model was a deliberate clean start: `channels.db` is a
new file, `channels` is the only table, and the previous `proxies.db` is
deleted per environment after the release. There was no data migration and no
transitional acceptance in `post-deploy.sh`.

Order per environment: **release first, hooks after**:

1. the release runs with the old hooks, which still match the old
   `proxies.db`; its pre-deploy makes the last backup of the old file;
2. `31_apps -e env=<env>` installs the hooks that require `channels.db` and
   `channels`;
3. delete the old `proxies.db` (the new image never opens it);
4. re-create channels from the old backup if they matter.

The new image creates `channels.db` on startup, so the new hooks are valid by
the time they are installed.

### 5.3 Compose, ports, volumes

| Change | Edit | Then |
|---|---|---|
| published port or bind address | `arch2debian/inventory/group_vars/debian_servers/exposure.yml` (`ytace_<env>_*`) | `15_firewall`, `30_services` (policy), `31_apps` (`.env`), restart/release |
| service, volume, healthcheck, logging | `deploy/compose/docker-compose.yml` | release; `40_verify` |
| data/cache directory layout | `roles/ytaceproxy/tasks/environment.yml`, defaults; compose uses `${DATA_DIR}` | `31_apps` |
| pre/post hook behaviour | `roles/ytaceproxy/templates/*.j2` | `31_apps`; next deploy uses them |
| environment-specific ports | `inventory/group_vars/debian_servers/main.yml` (`ytaceproxy_environments`) + `exposure.yml` | `15_firewall`, `30_services`, `31_apps`, restart/release |

Docker-published ports are tailnet-only in the current matrix. A LAN exposure
also needs an explicit firewall change; see `arch2debian/docs/operations.md`.

A compose change is a release, not an Ansible run. The policy checks the
resolved compose against the exposure matrix and rejects unknown ports, binds
outside `DATA_DIR`, host escapes, mutable images and non-`json-file` logging.

### 5.4 Companion images and manifest keys

The manager is built here. `acestream` and `acexy` are still pinned in the
workflow and shipped in the compose, although the application no longer uses
them (the `/ace` route was removed; their fate is a roadmap decision):

```yaml
env:
  ACESTREAM_IMAGE: ghcr.io/martinbjeldbak/acestream-http-proxy:3.5.1
  ACEXY_IMAGE: ghcr.io/javinator9889/acexy:0.2.2
```

When changing a companion version or adding/removing a service:

1. update the tag in `.github/workflows/release.yml`;
2. update `deploy/build_manifest.py` (`IMAGE_KEYS`) and the companions
   resolver if the image key set changes;
3. update `deploy/compose/docker-compose.yml` and
   `deploy/compose/env.required`;
4. update the allowlist in
   `arch2debian/roles/deploy/templates/deploy-app-root.j2` (exact key set and
   registry/repository regex);
5. update `roles/verify` and this documentation;
6. push `dev`, verify, then merge `main`.

The manifest key set is exact: `app`, `config`, `images`, `release`.
`images` has exactly `manager`, `acestream`, `acexy`; `config` has exactly
`compose_b64`, `compose_sha256` (over the decoded bytes, at most 256 KiB) and
`required_env` (copied from `deploy/compose/env.required`, sorted, unique). The
wrapper rejects a missing or extra key, a tag instead of a digest, a compose
hash mismatch and a missing `.env` name.

### 5.5 Deploy wrapper and verification

| Change | Edit |
|---|---|
| app name, release prefix, image allowlist | `arch2debian/roles/deploy/templates/deploy-app-root.j2` |
| compose directory map | `roles/deploy/defaults/main.yml` |
| expected containers/ports | `roles/verify/defaults/main.yml`, `inventory/.../exposure.yml` |
| deploy user, key, sudo, log rotation | `roles/deploy/*` |

Run `30_services` after changing the wrapper/policy; run `40_verify` after
changing expectations.

### 5.6 Secrets and CI configuration

- Application secrets, if added, belong in Ansible Vault and rendered into
  a `0600` `.env` or compose environment; never commit them.
- GitHub Actions needs only `DEPLOY_SSH_KEY`, `DEPLOY_HOST`, `DEPLOY_USER`,
  `DEPLOY_KNOWN_HOSTS`, `TS_OAUTH_CLIENT_ID`, `TS_OAUTH_SECRET`.
- Tailscale OAuth scope is `auth_keys`, tag `tag:ci`; the runner is ephemeral.
- The `deploy` user is not in the Docker group and has one forced command.
- The admin UI has no basic auth; tailnet binding is the access control.

### 5.7 Documentation

Update the document that states the contract in the same commit:

| Change | Document |
|---|---|
| endpoint, fallback, admin route | `README.md`, `docs/architecture.md` |
| database table/column | `docs/architecture.md`, `docs/development.md` |
| deploy flow, env, change matrix | `docs/deployment.md` |
| new failure and its fix | `docs/troubleshooting.md` |
| planned/removed feature | `docs/roadmap.md` |
| ownership/manifest contract | `arch2debian/docs/deployment-contract.md` |

## 6. Verification checklist

After a dev or prod deploy:

```bash
APP=ytaceproxy-dev
ENV=dev

# 1. release, compose hash and required env
ssh asus@debian "sudo jq . /var/lib/deploy/$APP/manifest.json"

# 2. deploy log
ssh asus@debian "sudo tail -n 80 /var/log/deploy/$APP.log"

# 3. containers and running image
ssh asus@debian "sudo /srv/$APP/compose.sh ps"
ssh asus@debian "sudo docker inspect -f '{{.Config.Image}}' $APP-manager-1"

# 4. UI and test redirects
curl -sS -o /dev/null -w '%{http_code}\n' http://<tailnet-ip>:<manager-port>/health
curl -sS -o /dev/null -w '%{http_code}\n' http://<tailnet-ip>:<manager-port>/admin/channels
curl -sS -o /dev/null -w '%{http_code} %{redirect_url}\n' \
  http://<tailnet-ip>:<manager-port>/play/<channel>

# 5. backup
ssh asus@debian "sudo ls -lt /var/backups/ytaceproxy/$ENV/ | head"

# 6. full contract check
cd arch2debian
.venv/bin/ansible-playbook playbooks/40_verify.yml
```

The acceptance criteria are: the manifest has exactly `manager`, `acestream`,
`acexy` and the three `config` keys; the compose file hash equals
`config.compose_sha256`; `required_env` is present in `.env`; the policy
passes; the manager is healthy; `/health` answers 200; `/admin/channels`
answers; a known `/play` channel is 302; a pre-deploy backup exists; running
images equal the manifest digests.

## 7. Rollback

### Image and compose rollback

The wrapper restores `manifest.prev.json` images and compose automatically on a
failed deploy. To replay the previous release through the forced command:

```bash
APP=ytaceproxy-dev
MANIFEST=/tmp/$APP-prev.json
ssh asus@debian "sudo cat /var/lib/deploy/$APP/manifest.prev.json" > "$MANIFEST"
RELEASE="$(jq -r .release "$MANIFEST")"
B64="$(base64 -w0 "$MANIFEST" | tr '+/' '-_' | tr -d '=')"
ssh -i ~/.config/arch2debian/deploy_key deploy@debian "$APP $RELEASE $B64"
```

### Database rollback

Only when the schema broke. Stop the manager, restore the backup, start it:

```bash
APP=ytaceproxy-dev
ENV=dev
ssh asus@debian "sudo /srv/$APP/compose.sh stop manager"
ssh asus@debian "sudo cp /var/backups/ytaceproxy/$ENV/pre-<ts>.db /var/lib/ytaceproxy-$ENV/manager-data/channels.db"
ssh asus@debian "sudo /srv/$APP/compose.sh start manager"
```

There are no migrations, so an image rollback does not roll back the schema;
the pre-deploy backup is the restore point. The single-table rollout was a
deliberate wipe: a pre-change backup contains the old `proxies.db` with
`redirect_channels`/`ace_channels`, not the new `channels` schema.

## 8. Known operational notes

- `30_services` never starts application stacks; `31_apps` writes the
  environment; the release starts the stack.
- The manager healthcheck is an HTTP `GET /health` inside the container: it
  proves the web app answers and SQLite is readable.
- The old `/app/data/proxies.db` is obsolete. Do not recreate it.
- Logs are capped per container (`json-file`, 10 MB × 3).
- Memory limits are intentionally not set; measured usage is far below the
  host limit.
- The `acestream`/`acexy` companions still run but nothing in the app uses
  them; removing or relocating them is a deferred roadmap decision.
- The old single-stack `/srv/ytaceproxy` and `/var/lib/danilaltd-ytaceproxy-dev`
  are not used by the current deployment.
