# Roadmap

This file records the next agreed changes. Completed work is removed rather
than kept as a changelog.

Done:

- migrated dependency management and the image build to `uv`;
- removed the legacy ACE table/stub and the `ACE Channels` admin title;
- added `/ace/{name}` → `acexy` redirects with a separate `ace_channels`
  table and admin page, tested on dev and prod;
- added `pytest` tests for URL construction, 404/503 and the fresh schema;
- dropped the empty `ace_streaming_channels` table on dev and on prod. The prod
  backup is `/var/backups/ytaceproxy/prod/pre-2026-11-19-133555.db`;
- moved the server compose into `deploy/compose/` and switched both stacks to
  manifest v2 and wrapper v2;
- added `/health` with a SQLite read check and switched the manager healthcheck
  from a TCP connect to it;
- tightened the post-deploy hook to require `redirect_channels` and
  `ace_channels` (applied with `31_apps` on dev and prod after `prod-12` proved
  the new table on both stacks).

Remaining work:

## 1. Remove the host-published acestream ports

`acexy` reaches `acestream` over the Docker network, so the host-published
`6878`/`6879` ports are only needed for direct engine access. After all clients
use `/ace` through `acexy`:

1. remove the `acestream` published port from
   `deploy/compose/docker-compose.yml`;
2. remove the corresponding `ytace_<env>_stream` entries from
   `arch2debian/inventory/group_vars/debian_servers/exposure.yml` and
   `arch2debian/roles/verify`;
3. run `15_firewall`, `30_services` (policy) and `31_apps` (`.env`), then cut a
   compose release and run `40_verify`;
4. smoke-test `/ace` in dev and prod.

Until then, direct access remains possible and bypasses acexy's multiplexing.

## 2. Structured logging

Add structured logging with request/channel fields, without logging resolved
URLs that contain credentials or ACE content ids.

## 3. Memory limits

Revisit memory limits after a week of `docker stats` data. Current usage is far
below the host limit, so this is not urgent.

## Order

1. Structured logging.
2. Acestream port removal (after clients migrate).
3. Memory limits.

Each application change follows the same release path: `dev` → verify → `main`
→ verify, with a pre-deploy SQLite backup and a green `40_verify`. Compose
changes go through `deploy/compose/docker-compose.yml`, not an Ansible
template.
