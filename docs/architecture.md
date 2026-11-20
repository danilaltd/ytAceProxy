# Architecture

The service resolves a source URL with `yt-dlp` and returns a 302 redirect to
the direct playback URL. Channels are managed through a small admin UI. The
manager never streams video.

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
                                      │ yt-dlp subprocess
                                      ▼
                             source URL resolution
                                      │
                                      └─ 302 Location ──▶ client
```

The deployed compose still contains the `acestream` and `acexy` companion
containers, but the application no longer references them: the `/ace` route,
`ace_channels` table and `ACEXY_BASE_URL` were removed. Their fate (host-level
Docker or removal) is a deferred roadmap decision.

## Request flow

### `GET /play/{channel}`

1. `handler.handle_channel` reads `{channel}` from the route.
2. `repo.get_redirect_url(name)` selects `redirect_url` from `channels` by
   name.
3. If it is not null, `redirect_response()` returns `302` with `Location`.
4. If the channel is unknown, the handler tries the special name
   `placeholder`.
5. If neither row exists, it returns `404`.
6. The response also sets a generic `User-Agent`, `Accept` and
   `Connection: keep-alive`; these are response headers, not a proxy.

### `GET /play/{channel}/upd`

`config.update_special_channel(name, appContext)` loads all channels, forces a
resolution for the requested name and writes changed rows back. The endpoint
returns `200 text/plain` even if the resolver failed; failures are logged.

### `GET /health`

`handler.health_handler` runs `db.check_db()`: one SQLite read of `channels`. A
readable database returns `200 ok`; any error returns `503 unhealthy`. The
manager healthcheck in `deploy/compose/docker-compose.yml` calls this endpoint,
so `docker compose up -d --wait` proves that the web app and the database both
work.

### Admin UI

`admin_routes.py` exposes one resource, `/admin/channels`:

- list with name, source URL, quality, redirect state, TTL badge, created;
- add/edit with a quality preset or a custom yt-dlp format selector;
- delete (POST, confirmation in the template);
- `Update now` per channel: starts `update_special_channel()` in the
  background and redirects immediately (a resolve can take up to 60 s);
- `Sync all`: starts `sync_channels()` in the background.

After add, and after an edit that changed the URL or format, the handler starts
a background resolve. The TTL badge is computed by `ttl.ttl_state()`:
`eternal` for `-1`, `valid` with a human duration, `expired`, or `unknown` for
`NULL`.

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
- `resolve_channels()` uses a semaphore of 2 (`MAX_PROCESSES`) and
  deduplicates by `(source URL, format)`, so two channels with the same URL and
  different quality resolve independently.
- `run_in_process_with_timeout()` returns `(url, format, link)`;
  `resolve_channels()` and the worker must keep that triple in sync (covered by
  `tests/test_resolver.py`).
- A channel whose `ttl` is `-1` or in the future is skipped unless the caller
  forces an update. A successful resolution extracts an expiry from the URL
  (`expire=`, `validto=`, or similar) and stores it in `ttl`.
- `update_channels()` writes only rows marked `dirty`.
- Logs never contain source or resolved URLs (they may carry tokens); errors
  are sanitised and only channel names/ttl are logged.

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

`format` is a nullable per-channel yt-dlp selector; empty means the default.
`redirect_url` is the resolved (or manually overridden) 302 target. `ttl` is the
expiry extracted from the resolved URL, `-1` for eternal, `NULL` for unknown.

There are **no migrations** and the old `proxies.db` is obsolete. The
single-table model was a clean start: a fresh database contains only
`channels`; the previous `redirect_channels`/`ace_channels` data was not
migrated and the old file was deleted per environment after the rollout.

## Deployment topology

Ansible defines two independent environments in
`inventory/group_vars/debian_servers/main.yml`:

| | prod | dev |
|---|---|---|
| app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream (unused by the app) | `6878` | `6879` |
| acexy (unused by the app) | `8090` | `8091` |
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

## ACE removal

The `/ace/{name}` route, `ace_channels` table, ACE admin pages, content-id
validation and `ACEXY_BASE_URL` were removed because ACE playback is not used.
The `acestream`/`acexy` containers remain deployed but idle. Moving them to a
host-level Docker project or removing them is a deferred decision in
[roadmap.md](roadmap.md).
