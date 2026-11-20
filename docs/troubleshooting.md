# Troubleshooting

Symptom → cause → command. These entries are real incidents from this
project, not generic advice.

## CI

### Gitleaks: `unknown revision` / `failed to scan Git repository`

Symptom in the `security-scan` job:

```text
git log ... 8313da4^..568c6fc
fatal: ambiguous argument ... unknown revision
failed to scan Git repository: "stderr is not empty"
```

`actions/checkout` fetched only the head commit, so the base of the push range
was not in the clone. The scan did not find a secret; it could not run.

Fix: the Gitleaks step needs full history:

```yaml
- uses: actions/checkout@v4
  with:
    fetch-depth: 0
```

Do not “fix” this by deleting the security job.

### Tailscale: `invalid key: unable to validate API key`

The action started `tailscaled`, but every `tailscale up` attempt failed:

```text
backend error: invalid key: unable to validate API key
sudo failed with exit code 1
```

This is auth-key validation, not the SSH ACL. Check, without printing secrets:

1. `TS_OAUTH_CLIENT_ID` and `TS_OAUTH_SECRET` are a pair from the same OAuth
   client and the same tailnet as `debian`;
2. the client scope is `auth_keys`;
3. the client tags match `tag:ci`, and `tag:ci` is in `tagOwners`;
4. `ping: debian` resolves in the tailnet.

Rotate the secret in the Tailscale admin console, update both GitHub secrets,
then rerun the failed job. Do not change the ACL blindly.

### Deploy job: `usage: ssh deploy@host ...`

The forced command received the wrong number of arguments. The only accepted
form is:

```text
<app> <release> <manifest-base64url>
```

Check the workflow `APP`, `RELEASE` and `manifest.b64`; an empty or multiline
manifest also fails the encoding check.

### Manifest rejected: `manifest image ref rejected`

The wrapper accepts only digest references from the per-app allowlist:

```text
ghcr.io/danilaltd/ytaceproxy-proxy@sha256:<64 hex>
ghcr.io/martinbjeldbak/acestream-http-proxy@sha256:<64 hex>
ghcr.io/javinator9889/acexy@sha256:<64 hex>
```

A tag, a foreign repository, a missing key or an extra key is rejected before
Docker. Check the workflow manifest assertion and
`arch2debian/roles/deploy/templates/deploy-app-root.j2`.

### First release: `missing compose file`

A stack that has never been released has no `/srv/<app>/docker-compose.yml`.
That is expected: `30_services` is host-only and `31_apps` writes only `.env`,
hooks and `compose.sh`. The first release writes compose and starts the stack.
Do not create a compose file by hand.

### `compose sha256 mismatch` / `required env ... missing`

`config.compose_sha256` must match `deploy/compose/docker-compose.yml`, and
every name from `deploy/compose/env.required` must exist non-empty in
`/srv/<app>/.env`.

```bash
sha256sum deploy/compose/docker-compose.yml
ssh asus@debian "sudo jq -r .config.compose_sha256 /var/lib/deploy/ytaceproxy-dev/manifest.json"
ssh asus@debian "sudo jq -r '.config.required_env[]' /var/lib/deploy/ytaceproxy-dev/manifest.json"
ssh asus@debian "sudo grep -E '^(DATA_DIR|TAILSCALE_IP|ACEXY_BASE_URL)=' /srv/ytaceproxy-dev/.env"
```

For a hash mismatch, re-release the branch that owns the compose. For a
missing name, update the Ansible `.env` template/Vault and run
`31_apps -e env=dev|prod` before retrying.

## Ansible

### `compose policy rejected`

The wrapper resolves the compose and runs
`/usr/local/libexec/deploy-compose-policy` before applying it. It rejects host
escapes (`privileged`, `network_mode: host`, `pid`/`ipc: host`, `devices`,
`cap_add`), `build`, binds outside `DATA_DIR`, ports outside `arch2debian`'s
`exposure.yml`, mutable image tags and logging that is not `json-file`
10m × 3. `env_file` is a source-review rule: Compose merges it into
`environment` before the wrapper sees the resolved config, and the current CI
does not scan the source.

```bash
ssh asus@debian "sudo tail -80 /var/log/deploy/ytaceproxy-dev.log"
```

Fix `deploy/compose/docker-compose.yml` and release it. If the port is new,
update `exposure.yml`, run `15_firewall` and `30_services` (policy), then
`31_apps` (`.env`).

### `31_apps` refuses to run

`env=prod` and `env=all` require a clean `main` checkout of `arch2debian`;
commit/stash or use `env=dev`. Tailscale must be running with an IPv4 address,
because `.env` needs concrete binds.

### acestream restarts in a loop

```text
Exception: cache dir is not writable: /home/appuser/.ACEStream/cache
PermissionError: ... /home/appuser/.ACEStream/...
```

The container runs as uid/gid 1000, but the bind-mounted cache directory is
owned by root. The role must create it with the image user:

```yaml
owner: "{{ ytaceproxy_acestream_uid }}"
group: "{{ ytaceproxy_acestream_gid }}"
```

Run `31_apps -e env=dev|prod` (the role creates the cache directory). Do not
chown `/var/lib/...` by hand.

### Pre-deploy backup fails with `Could not find the file`

Symptom:

```text
Error response from daemon: Could not find the file /app/data/.pre-deploy-backup.db
```

`docker exec` without `-i` does not feed the heredoc to Python, so the backup
script never runs. The hook must use:

```bash
docker exec -i "$C" python - <<'PY'
...
PY
```

Test it manually:

```bash
ssh asus@debian "sudo /srv/ytaceproxy-dev/pre-deploy.sh manual-test"
ssh asus@debian "sudo ls -l /var/backups/ytaceproxy/dev/"
```

### Post-deploy failed

The wrapper prints `=== deploy failed` and rolls the image back to
`manifest.prev.json`.

```bash
ssh asus@debian "sudo tail -n 120 /var/log/deploy/ytaceproxy-dev.log"
```

The post-deploy hook checks `PRAGMA integrity_check`, the `channels` table and
the channel count. A failed check means the new image cannot use the current
database. The image and compose are rolled back; the database is **not**.
Restore the last `pre-*.db` backup if the schema is incompatible.

## Application

### `/health` returns 503

The manager serves, but SQLite is unreadable (wrong bind mount, a corrupt
database or a missing `channels` table). The container healthcheck fails and
`docker compose up -d --wait` blocks, so a deploy in this state rolls back.
Check:

```bash
ssh asus@debian "sudo /srv/ytaceproxy-dev/compose.sh ps"
ssh asus@debian "sudo docker inspect -f '{{json .State.Health}}' ytaceproxy-dev-manager-1"
curl -sS -o /dev/null -w '%{http_code}\n' http://<tailnet-ip>:8089/health
```

Fix the volume or restore the pre-deploy backup; do not edit `/srv` by hand.

### `/play/<name>` returns 404

Expected when:

- the channel row does not exist, and
- no row named `placeholder` has a `redirect_url`.

Check the database:

```bash
ssh asus@debian "sudo docker exec -i ytaceproxy-dev-manager-1 python - <<'PY'
import sqlite3
c = sqlite3.connect('/app/data/channels.db')
print(c.execute('select id,name,url,format,redirect_url,ttl from channels').fetchall())
PY"
```

Add or fix the channel at `/admin/channels`, or add a `placeholder` row.

### 2026-11-20: a new channel stayed at 404 after the single-table rollout

Symptom: `/play/qq` returned 404, and `nsf` kept an expired link even after
“Update now”. The admin showed no redirect for `qq`.

Cause: the resolver refactor changed `run_in_process_with_timeout()` to return
`(url, link)` while `resolve_channels()` unpacked `(url, format, link)`. Every
successful resolve raised `ValueError: not enough values to unpack`, so the
redirect and TTL were never written. Failed resolves were handled; successful
ones crashed. The manager log showed `Task exception was never retrieved`.

Why it shipped: the unit tests stubbed background resolution and never
exercised the real worker contract; the smoke test used a redirect override,
which bypasses the resolver; CI did not run tests at all.

Fix: the worker returns `(url, format, link)`; `tests/test_resolver.py` covers
the worker contract, persistence through `update_special_channel()` and
`sync_channels()`, dedup, skips and failures. The release workflow now has a
`test` job that gates the build.

Prevention: any resolver/DB/route change needs a test; do not validate only
with a redirect override; keep `uv run pytest` green before pushing.

For a channel that stays unresolved, check the logs:

```bash
ssh asus@debian "sudo docker logs --tail 100 ytaceproxy-prod-manager-1 2>&1 | grep -Ei 'resolve|Traceback' | tail"
```

A failed `yt-dlp` resolve leaves `redirect_url` empty; fix the source URL or
quality and press “Update now”.

### `/play/<name>` returns 302 but the link is expired

`ttl` is in the past. Use `/play/<name>/upd` or the admin “Update now” button to
re-resolve. If `yt-dlp` fails, the old `redirect_url` remains; check the manager
logs.

### A quality selector is rejected

Merge selectors (`bestvideo+bestaudio` and anything with `+`) need ffmpeg,
which the image does not include. Use a single-format selector such as
`best[height<=720]`. The add/edit form rejects `+` for this reason.

### The player still points at `/yt_dlp`

The old route is removed. Update the playlist/player URLs to `/play/<name>`;
unknown paths return 404.

### `uv sync --frozen` fails after changing dependencies

`pyproject.toml` and `uv.lock` are out of sync. Run `uv lock`, commit
`uv.lock`, then `uv sync --frozen`. Do not edit `uv.lock` by hand and do not
re-add `requirements.txt`.

### Docker build cannot download `uv`

The Dockerfile pins `ghcr.io/astral-sh/uv:<version>` as a build stage. If the
tag is unavailable, update the pin in `Dockerfile` and rebuild; do not replace
it with `latest`.

### Admin UI returns 500 after a local run

Templates and static assets are loaded package-relative (`playgate/forms`,
`playgate/static`), so the working directory no longer matters. If the page
still fails, check that the package was copied into the image (`Dockerfile`
`COPY . ./`) and read the traceback in the logs.

### Admin UI is unreachable

The deployed UI binds to the Tailscale IP, not the LAN:

```bash
ssh asus@debian "sudo ss -ltnp | grep -E ':(8088|8089)'"
curl -sS -o /dev/null -w '%{http_code}\n' http://<tailnet-ip>:8089/admin/channels
```

Check that Tailscale is running on the client and that the port belongs to the
expected environment. There is no basic auth by design.

### Container is running an old image

Compare the running image with the manifest:

```bash
APP=ytaceproxy-dev
ssh asus@debian "sudo jq -r .images.manager /var/lib/deploy/$APP/manifest.json"
ssh asus@debian "sudo docker inspect -f '{{.Config.Image}}' $APP-manager-1"
```

If they differ, the deploy did not complete or a manual `docker compose` bypassed
`compose.sh` and `deploy-images.env`. Check the deploy log and rerun the release;
do not `docker pull` a tag by hand. `40_verify` also compares the compose hash,
required env and running images with the manifest.

### SQLite is read-only or locked

The manager normally runs as root in the current image. A read-only error
usually means the bind mount is wrong or the host directory is not writable.
Check:

```bash
ssh asus@debian "sudo ls -ld /var/lib/ytaceproxy-dev/manager-data"
ssh asus@debian "sudo docker inspect -f '{{json .Mounts}}' ytaceproxy-dev-manager-1"
```

Fix the role/volume, not the live container.

### Old `proxies.db` / `channels.db` confusion

`/app/data/channels.db` is the current SQLite file with the single `channels`
table. The old `/app/data/proxies.db` (`redirect_channels`/`ace_channels`) and
the JSON-era `channels.db` copies are obsolete. Do not restore either; a
pre-change backup is only for disaster recovery of the previous model.

## Where to look next

- deployment and change matrix: [deployment.md](deployment.md)
- runtime/data model: [architecture.md](architecture.md)
- planned health/logging and acestream exposure work: [roadmap.md](roadmap.md)
- Ansible-side incidents: `arch2debian/docs/ci-cd.md`,
  `arch2debian/roles/ytaceproxy/README.md`
- ownership and manifest: `arch2debian/docs/deployment-contract.md`
