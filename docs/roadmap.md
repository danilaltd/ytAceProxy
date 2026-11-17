# Roadmap

This file records the next agreed changes. Completed work is removed rather
than kept as a changelog.

Done on `dev`:

- migrated dependency management and the image build to `uv`;
- removed the legacy ACE table/stub and the `ACE Channels` admin title;
- added `/ace/{name}` → `acexy` redirects with a separate `ace_channels`
  table and admin page;
- added `pytest` tests for URL construction, 404/503 and the fresh schema;
- dropped the empty `ace_streaming_channels` table on the dev database after a
  pre-deploy backup. Prod still has the empty table until its rollout.

Remaining work:

## 1. Cleanup of the legacy table on prod

New databases no longer create `ace_streaming_channels`. Dev was cleaned after
a pre-deploy backup; the prod database still contains the empty orphan. Drop it
only after the prod rollout and a backup, and record the result in the
deployment log.

```bash
ssh asus@debian "sudo /srv/ytaceproxy-prod/pre-deploy.sh pre-ace-cleanup"
ssh asus@debian "sudo docker exec -i ytaceproxy-prod-manager-1 python - <<'PY'
import sqlite3
c = sqlite3.connect('/app/data/proxies.db')
c.execute('DROP TABLE IF EXISTS ace_streaming_channels')
c.commit()
c.close()
PY"
```

Verify with `SELECT name FROM sqlite_master WHERE type='table'`; an image
rollback to the old code may recreate the empty table, which is harmless.

## 2. Remove the host-published acestream ports

`acexy` reaches `acestream` over the Docker network, so the host-published
`6878`/`6879` ports are only needed for direct engine access. After all clients
use `/ace` through `acexy`:

1. remove the `acestream` published port from
   `arch2debian/roles/ytaceproxy/templates/docker-compose.yml.j2`;
2. remove the corresponding `ytace_<env>_stream` entries from
   `inventory/group_vars/debian_servers/exposure.yml` and `roles/verify`;
3. update the architecture/deployment docs and the local compose only if the
   local stack should match;
4. run `30_services` and `40_verify`, then smoke-test `/ace` in dev and prod.

Until then, direct access remains possible and bypasses acexy's multiplexing.

## 3. Health endpoint and structured logging

- Add `/health` that checks SQLite read access; use it in the manager
  healthcheck if it is more useful than the TCP connect.
- Add structured logging with request/channel fields, without logging resolved
  URLs that contain credentials or ACE content ids.

## 4. Memory limits

Revisit memory limits after a week of `docker stats` data. Current usage is far
below the host limit, so this is not urgent.

## Order

1. Optional legacy-table cleanup (data-only, no release).
2. Health endpoint + logging.
3. Acestream port removal (after clients migrate).
4. Memory limits.

Each application change follows the same release path: `dev` → verify → `main`
→ verify, with a pre-deploy SQLite backup and a green `40_verify`.
