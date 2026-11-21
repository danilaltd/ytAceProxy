# Architecture

The service resolves an http(s) source URL with `yt-dlp` and returns a 302
redirect to the direct playback URL. Sources stored as `acestream://<40 hex>`
are redirected to the `acexy` stream endpoint instead. Channels are managed
through a small admin UI. The manager never streams video.

## Components

```text
                       ┌─────────────────────────────┐
client ──HTTP─────────▶│ manager (aiohttp, port 80)  │
                       │  /play, /admin, /health     │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │ SQLite /app/data/channels.db│
                       │ channels                    │
                       └──────────────┬──────────────┘
                                      │
                    ┌─────────────────┴──────────────────┐
                    │ http(s): yt-dlp subprocess         │ acestream://: 302
                    ▼                                    ▼
             source URL resolution                 acexy (port 80)
                    │                                    └─▶ acestream (6878)
                    └─ 302 Location ──▶ client
```

The deployed compose contains the `acestream` and `acexy` companion containers;
ACE channels reach them through `acexy`. The manager never streams video: it
only resolves or redirects.

## Request flow

### `GET /play/{channel}`

1. `handler.handle_channel` reads `{channel}` from the route.
2. `repo.get_channel_by_name(name)` returns the row.
3. `sources.parse_source(url)` selects the handler:
   - http(s) → `yt-dlp` path: `redirect_url` is returned as `302`; an
     unresolved row falls back to the `placeholder` channel, then `404`;
   - `acestream://<40 hex>` → `302` to
     `<acexy origin>/ace/getstream?id=<hash>`; a missing/incorrect origin is
     `503` (no `placeholder` fallback); malformed sources fall through to the
     placeholder path and are logged;
4. The `acexy` origin is `ACEXY_BASE_URL` if set, otherwise
   `http://{TAILSCALE_IP}:{ACEXY_PORT}`; the client must be able to reach it.
5. The response also sets a generic `User-Agent`, `Accept` and
   `Connection: keep-alive`; these are response headers, not a proxy.

### `GET /play/{channel}/upd`

`config.update_special_channel(name, appContext)` loads all channels, forces a
resolution for the requested name and writes changed rows back. Only http(s)
sources are resolved; an ACE channel (or any non-yt-dlp source) returns
`200 text/plain` with "nothing to resolve".

### `GET /health`

`handler.health_handler` runs `db.check_db()`: one SQLite read of `channels`. A
readable database returns `200 ok`; any error returns `503 unhealthy`. The
manager healthcheck in `deploy/compose/docker-compose.yml` calls this endpoint,
so `docker compose up -d --wait` proves that the web app and the database both
work.

### Admin UI

`admin_routes.py` exposes one resource, `/admin/channels`:

- list with name, kind badge, source, quality, redirect state, TTL badge,
  created;
- add/edit with a single Source field: an http(s) URL, `acestream://<40 hex>`,
  or a bare 40-hex content id (normalized to `acestream://`);
- for http(s) sources: a quality preset or a custom yt-dlp format selector, and
  an optional redirect override; those fields are hidden for ACE sources;
- delete (POST, confirmation in the template);
- `Update now` per channel (hidden for ACE): starts
  `update_special_channel()` in the background and redirects immediately;
- `Sync all`: starts `sync_channels()` in the background.

After add, and after an edit that changed the URL, format or source scheme,
the handler starts a background resolve for yt-dlp sources. Changing the source
scheme on edit resets `format`, `redirect_url` and `ttl`. The TTL badge is
computed by `ttl.ttl_state()`; ACE rows show `n/a`.

The admin UI is deliberately unauthenticated because the published port is
bound to the Tailscale IP. Do not expose it on the LAN or the internet.

## Resolver

`config.py` owns yt-dlp URL resolution.

- `python_yt_dlp_get_link(url, fmt)` uses
  `yt-dlp --quiet --skip-download --format <fmt-or-default>`; the default is
  `best[acodec!=none]`. Merge selectors (`+`) are rejected: the image has no
  ffmpeg.
- `run_in_process_with_timeout()` runs the resolver in a `multiprocessing`
  process and polls the queue; the default timeout is 60 seconds.
- `resolve_channels()` filters to http(s) sources, uses a semaphore of 2
  (`MAX_PROCESSES`) and deduplicates by `(source URL, format)`, so two channels
  with the same URL and different quality resolve independently. ACE sources
  (`acestream://`) never reach the resolver.
- `run_in_process_with_timeout()` returns `(url, format, link)`;
  `resolve_channels()` and the worker must keep that triple in sync (covered by
  `tests/test_resolver.py`).
- A channel whose `ttl` is `-1` or in the future is skipped unless the caller
  forces an update. A successful resolution extracts an expiry from the URL
  (`expire=`, `validto=`, or similar) and stores it in `ttl`.
- `update_channels()` writes only rows marked `dirty`.
- Logs never contain source or resolved URLs or ACE content ids (they may
  carry tokens or private ids); errors are sanitised and only channel
  names/ttl are logged.

The resolver is deliberately out-of-process: `yt-dlp` is synchronous, can block
for seconds and can spawn its own work. Running it inline would block the
`aiohttp` loop.

## Data model

`playgate/db.py` creates the only table on startup with
`CREATE TABLE IF NOT EXISTS`:

```sql
channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    format TEXT,
    redirect_url TEXT,
    ttl INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

`url` is the source URI: an http(s) URL for `yt-dlp`, or the native
`acestream://<40 hex>` URI for ACE playback. `format` is a nullable per-channel
yt-dlp selector; empty means the default and it is `NULL` for ACE.
`redirect_url` is the resolved (or manually overridden) 302 target and is
`NULL` for ACE. `ttl` is the expiry extracted from the resolved URL, `-1` for
eternal, `NULL` for unknown or ACE.

There are **no migrations** and the old `proxies.db` is obsolete. The
single-table model was a clean start: a fresh database contains only
`channels`; the previous `redirect_channels`/`ace_channels` data was not
migrated and the old file was deleted per environment after the rollout. ACE
playback returned later as a source scheme in the same table — no second table
and no schema change.

## Deployment topology

Ansible defines two independent environments in
`inventory/group_vars/debian_servers/main.yml`:

| | prod | dev |
|---|---|---|
| app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream (used by ACE channels) | `6878` | `6879` |
| acexy (used by ACE channels) | `8090` | `8091` |
| data | `/var/lib/ytaceproxy-prod` | `/var/lib/ytaceproxy-dev` |
| backups | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |

Both stacks can run simultaneously. They share no database, cache, manifest or
port. The manager container names follow `ytaceproxy-<env>-manager-1`.

The manager image is built from this repository. The `acestream` and `acexy`
images are still pinned in `.github/workflows/release.yml` and resolved to
digests by the `companions` job. The server compose shape lives in
`deploy/compose/docker-compose.yml` and is embedded in release manifest v2 with
its sha256 and `required_env`. Ansible owns `.env`, hooks and `compose.sh`; the
wrapper writes `/srv/<app>/docker-compose.yml` and `deploy-images.env`.
`30_services` never writes application compose and never starts application
containers.

## Lifecycle and failure behaviour

- `docker compose up -d --wait` waits for the manager healthcheck (an HTTP
  `GET /health` inside the container: the web app answers and SQLite is
  readable) and for running containers.
- `pre-deploy.sh` creates a consistent SQLite backup before the image switch.
  It skips the backup only when the manager is not running or
  `/app/data/channels.db` does not exist yet.
- `post-deploy.sh` checks `PRAGMA integrity_check`, the `channels` table and
  the channel count. A failure makes the deploy wrapper roll the **image and
  compose** back to `manifest.prev.json`; the database is not rolled back
  automatically.
- `31_apps -e env=dev|prod` writes `.env`, hooks and `compose.sh`; it never
  writes compose and never starts containers. The wrapper writes
  `/srv/<app>/docker-compose.yml` and `deploy-images.env` from the v2 manifest
  and validates the resolved compose before applying it.
- A stack without a manifest has no deployed compose; the first release creates
  it. `.env` carries host values only, never image references.
- Every service uses the `json-file` log driver with `max-size=10m` and
  `max-file=3`; the wrapper policy enforces it.

## ACE sources

ACE playback is a source scheme, not a separate route or table: a channel whose
`url` is `acestream://<40 hex>` is redirected to
`<acexy origin>/ace/getstream?id=<hash>`. There is no `/ace` route and no
`ace_channels` table. `ACEXY_BASE_URL` is an optional override; by default the
origin is `http://{TAILSCALE_IP}:{ACEXY_PORT}` from the host `.env`. ACE
channels are never resolved by `yt-dlp`; the admin hides quality/override/update
controls for them.

The `acestream`/`acexy` companions are part of the deployed stack again. Moving
them to a host-level Docker project remains a deferred roadmap decision; only
the "remove entirely" option is off the table while ACE channels exist.
