# Architecture

The service has two responsibilities: resolve a source URL with `yt-dlp` and
return a 302 redirect; map a named ACE channel to a 302 redirect into `acexy`.
Both mappings are managed through a small admin UI. Video playback is handled by
the companion `acexy`/`acestream` stack, not by this application.

## Components

```text
                       ┌─────────────────────────────┐
client ──HTTP─────────▶│ manager (aiohttp, port 80)  │
                       │  /yt_dlp, /ace, /admin/*    │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │ SQLite /app/data/proxies.db │
                       │ redirect_channels           │
                       │ ace_channels                │
                       └──────────────┬──────────────┘
                                      │
                 ┌────────────────────┴───────────────────┐
                 │ yt-dlp subprocess                      │ 302 Location
                 ▼                                        ▼
        source URL resolution                     acexy (port 80)
                                                  └─▶ acestream engine (6878)
```

In a deployed environment the three containers share a compose project and a
private Docker network, but only the manager and `acexy` publish host ports.
`acestream` is reachable by `acexy` under the service name `acestream`. The
manager never proxies video: `/ace/{name}` returns a redirect and the media
connection is between the client and `acexy`/`acestream`.

## Request flow

### `GET /yt_dlp/{channel}`

1. `handler.handle_yt_dlp` reads `{channel}` from the route.
2. `repo.get_redirect_url(name)` selects `redirect_url` from
   `redirect_channels` by name.
3. If it is not null, `redirect_response()` returns `302` with `Location`.
4. If the channel is unknown, the handler tries the special name
   `placeholder`.
5. If neither row exists, it returns `404`.
6. The response also sets a generic `User-Agent`, `Accept` and
   `Connection: keep-alive`; these are response headers, not a proxy.

### `GET /yt_dlp/{channel}/upd`

`config.update_special_channel(name, appContext)` loads all redirects, forces a
resolution for the requested name and writes changed rows back. The endpoint
returns `200 text/plain` even if the resolver failed; failures are logged.

### `GET /ace/{channel}`

1. `handler.ace_handler` reads `{channel}` from the route.
2. `repo.get_ace_content_id(name)` selects `content_id` from `ace_channels`.
3. An unknown name returns `404`.
4. `ace.build_acexy_stream_url()` builds
   `<ACEXY_BASE_URL>/ace/getstream?id=<urlencoded content_id>`.
5. A missing or invalid `ACEXY_BASE_URL` returns `503`; the content id is not
   written to the logs.
6. `redirect_response()` returns `302`. The client connects to `acexy`
   directly.

`ACEXY_BASE_URL` is rendered by Ansible from the per-environment published
acexy address (prod `8090`, dev `8091`). It must be reachable by the client;
the Docker DNS name `acexy` is not.

`/ace` names are a separate namespace from `/yt_dlp` names. A name can have a
yt-dlp redirect and an ACE content id at the same time.

### `GET /health`

`handler.health_handler` runs `db.check_db()`: one SQLite read of
`redirect_channels`. A readable database returns `200 ok`; any error returns
`503 unhealthy` and is logged without credentials. The manager healthcheck in
`deploy/compose/docker-compose.yml` calls this endpoint instead of a bare TCP
connect, so `docker compose up -d --wait` proves that the web app and the
database both work.

### Admin UI

`admin_routes.py` exposes two lists:

- `/admin/redirects` — yt-dlp redirects; add/edit/delete/sync. After add, the
  handler starts a background `sync_channels()` task. The sync route does the
  same for all rows.
- `/admin/ace` — ACE channels; add/edit/delete. A channel is only a name and a
  40-character content id; there is no resolver and no TTL.

The admin UI is deliberately unauthenticated because the published port is
bound to the Tailscale IP. Do not expose it on the LAN or the internet.

## Resolver

`config.py` owns yt-dlp URL resolution.

- `python_yt_dlp_get_link(url)` uses
  `yt-dlp --quiet --skip-download --format best[acodec!=none]`.
- `run_in_process_with_timeout()` runs the resolver in a `multiprocessing`
  process and polls the queue; the default timeout is 60 seconds.
- `parallel_fill_redirects()` uses a semaphore of 2 (`MAX_PROCESSES`) and
  deduplicates by source URL.
- A redirect whose `ttl` is `-1` or in the future is skipped unless the caller
  forces an update. A successful resolution extracts an expiry from the URL
  (`expire=`, `validto=`, or similar) and stores it in `ttl`.
- `update_redirects()` writes only rows marked `dirty`.

The resolver is deliberately out-of-process: `yt-dlp` is synchronous, can block
for seconds and can spawn its own work. Running it inline would block the
`aiohttp` loop.

## Data model

`proxy_ace/db.py` creates both tables on startup with
`CREATE TABLE IF NOT EXISTS`:

```sql
redirect_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    redirect_url TEXT,
    ttl INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

ace_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    content_id TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

`redirect_channels` is the yt-dlp table. `ace_channels` stores the AceStream
content id/hash used by `/ace/{name}`. The old `ace_streaming_channels` table
is no longer created and was dropped on dev and prod (prod backup:
`/var/backups/ytaceproxy/prod/pre-2026-11-19-133555.db`). The post-deploy hook
still accepts it during the transition; see [roadmap.md](roadmap.md).

There are **no migrations**. `init_db()` only creates missing tables. Adding
`ace_channels` is additive and works on an existing database. If a column
changes, an existing database will not be altered automatically. The safe
procedure is:

1. run a pre-deploy backup (`/srv/ytaceproxy-<env>/pre-deploy.sh manual`);
2. deploy the new image;
3. if the schema is incompatible, restore `proxies.db` from
   `/var/backups/ytaceproxy/<env>/pre-*.db`.

## Deployment topology

Ansible defines two independent environments in
`inventory/group_vars/debian_servers/main.yml`:

| | prod | dev |
|---|---|---|
| app | `ytaceproxy-prod` | `ytaceproxy-dev` |
| branch | `main` | `dev` |
| manager UI | `<tailnet-ip>:8088` | `<tailnet-ip>:8089` |
| acestream | `6878` | `6879` |
| acexy | `8090` | `8091` |
| `ACEXY_BASE_URL` | `http://<tailnet-ip>:8090` | `http://<tailnet-ip>:8091` |
| data | `/var/lib/ytaceproxy-prod` | `/var/lib/ytaceproxy-dev` |
| backups | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |

Both stacks can run simultaneously. They share no database, cache, manifest or
port. The manager container names follow `ytaceproxy-<env>-manager-1`.

The manager image is built from this repository. The `acestream` and `acexy`
images are pinned in `.github/workflows/release.yml` and resolved to digests by
the `companions` job; they are not built here. The server compose shape lives
in `deploy/compose/docker-compose.yml` and is embedded in release manifest v2
with its sha256 and `required_env`. Ansible owns `.env`, hooks and `compose.sh`;
the wrapper writes `/srv/<app>/docker-compose.yml` and `deploy-images.env`.
`30_services` never writes application compose and never starts application
containers.

## Lifecycle and failure behaviour

- `docker compose up -d --wait` waits for the manager healthcheck (an HTTP
  `GET /health` inside the container: the web app answers and SQLite is
  readable) and for running containers.
- `pre-deploy.sh` creates a consistent SQLite backup before the image switch.
  It skips the backup only when the manager is not running or `proxies.db`
  does not exist yet.
- `post-deploy.sh` checks `PRAGMA integrity_check`, `redirect_channels`,
  `ace_channels` when required by the role, and the redirect count. A failure
  makes the deploy wrapper roll the **image** back to `manifest.prev.json`;
  the database is not rolled back automatically.
- `31_apps -e env=dev|prod` writes `.env`, hooks and `compose.sh`; it never
  writes compose and never starts containers. The wrapper writes
  `/srv/<app>/docker-compose.yml` and `deploy-images.env` from the v2 manifest
  and validates the resolved compose before applying it.
- A stack without a manifest has no deployed compose; the first release creates
  it. `.env` carries host values only, never image references.
- Every service uses the `json-file` log driver with `max-size=10m` and
  `max-file=3`; the wrapper policy enforces it.

## ACE redirect

`/ace/{name}` replaces the old ACE streaming path. The old `/ace/{channel}`
video-streaming implementation is gone; the current route only redirects to
`acexy`. Content ids live in `ace_channels`, not in `redirect_channels`.

The legacy `ace_streaming_channels` table was dropped on dev and prod (prod
backup: `/var/backups/ytaceproxy/prod/pre-2026-11-19-133555.db`). The
post-deploy hook still accepts either table during the transition. The removal
of the host-published `acestream` port is tracked in
[roadmap.md](roadmap.md).
