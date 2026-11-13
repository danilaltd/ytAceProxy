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

### Deploy job: `missing compose file`

`30_services` was not run on the server, so `/srv/<app>/docker-compose.yml`
does not exist. Run `30_services.yml -e ansible_host=debian`, then retry.

## Ansible

### `30_services` downgraded the image to the bootstrap tag

Symptom: after a CI deploy, running `30_services` replaced the digest with the
bootstrap tag (`dev-6` / `prod-4`).

Cause: the role ignored `/var/lib/deploy/<app>/manifest.json`.

Check:

```bash
ssh asus@debian "sudo jq . /var/lib/deploy/ytaceproxy-dev/manifest.json"
ssh asus@debian "sudo cat /srv/ytaceproxy-dev/deploy-images.env"
```

The current role writes `deploy-images.env` when a manifest exists and passes
it to compose as an env file. If the file is missing, the role is old or the
playbook did not reach the task. Do not hand-edit the env file; fix the role
and rerun `30_services`.

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

Rerun `30_services`. Do not chown `/var/lib/...` by hand.

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

The post-deploy hook checks `PRAGMA integrity_check`, the required tables and
the redirect count. A failed check means the new image cannot use the current
database. The image is rolled back; the database is **not**. Restore the last
`pre-*.db` backup if the schema is incompatible.

## Application

### `/yt_dlp/<name>` returns 404

Expected when:

- the channel row does not exist, and
- no row named `placeholder` has a `redirect_url`.

Check the database:

```bash
ssh asus@debian "sudo docker exec -i ytaceproxy-dev-manager-1 python - <<'PY'
import sqlite3
c = sqlite3.connect('/app/data/proxies.db')
print(c.execute('select id,name,url,redirect_url,ttl from redirect_channels').fetchall())
PY"
```

Add or fix the redirect in `/admin/redirects`, or add a `placeholder` row.

### `/yt_dlp/<name>` returns 302 but the link is expired

`ttl` is in the past. Use `/yt_dlp/<name>/upd` or the admin sync action to
re-resolve. If `yt-dlp` fails, the old `redirect_url` remains; check the manager
logs.

### Admin UI returns 500 after a local run

`aiohttp_jinja2` loads templates with `FileSystemLoader('proxy_ace/forms')`,
relative to the process working directory. Run the app from the repository
root, or use the Docker image where `WORKDIR /app` is set.

### Admin UI is unreachable

The deployed UI binds to the Tailscale IP, not the LAN:

```bash
ssh asus@debian "sudo ss -ltnp | grep -E ':(8088|8089)'"
curl -sS -o /dev/null -w '%{http_code}\n' http://<tailnet-ip>:8089/admin/redirects
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

If they differ, the deploy did not complete or a manual `compose up` used a
bootstrap tag. Check the deploy log and rerun the release; do not `docker pull`
a tag by hand.

### SQLite is read-only or locked

The manager normally runs as root in the current image. A read-only error
usually means the bind mount is wrong or the host directory is not writable.
Check:

```bash
ssh asus@debian "sudo ls -ld /var/lib/ytaceproxy-dev/manager-data"
ssh asus@debian "sudo docker inspect -f '{{json .Mounts}}' ytaceproxy-dev-manager-1"
```

Fix the role/volume, not the live container.

### Old `channels.db` confusion

`channels.db` is the old JSON-era store. The current service uses
`/app/data/proxies.db`. Do not copy `channels.db` into the data directory and do
not restore its rows into `redirect_channels`; the formats are unrelated.

## Where to look next

- deployment and change matrix: [deployment.md](deployment.md)
- runtime/data model: [architecture.md](architecture.md)
- planned ACE/acexy/uv work: [roadmap.md](roadmap.md)
- Ansible-side incidents: `arch2debian/docs/ci-cd.md`,
  `arch2debian/roles/ytaceproxy/README.md`
