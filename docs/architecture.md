# Architecture

The service has two responsibilities: resolve a source URL with `yt-dlp` and
return a 302 redirect; manage the named redirects through a small admin UI.
Video playback is handled by the companion `acexy`/`acestream` stack, not by
this application.

## Components

```text
                       ┌─────────────────────────────┐
client ──HTTP─────────▶│ manager (aiohttp, port 80)  │
                       │  /yt_dlp, /admin/redirects  │
                       └──────────────┬──────────────┘
                                      │
                       ┌──────────────▼──────────────┐
                       │ SQLite /app/data/proxies.db │
                       │ redirect_channels           │
                       └──────────────┬──────────────┘
                                      │ yt-dlp subprocess
                                      ▼
                              source URL resolution

client ──HTTP─────────▶ acexy (port 80) ──▶ acestream engine (6878)
                       companion containers, separate from the manager
```

In a deployed environment the three containers share a compose project and a
private Docker network, but only the manager and `acexy` publish host ports.
`acestream` is reachable by `acexy` under the service name `acestream`.

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

### Admin UI

`admin_routes.py` exposes list/add/edit/delete/sync routes. The UI is a
Jinja2 template under `proxy_ace/forms/`. After add, the handler starts a
background `sync_channels()` task. The sync route does the same for all rows.

The admin UI is deliberately unauthenticated because the published port is
bound to the Tailscale IP. Do not expose it on the LAN or the internet.

## Resolver

`config.py` owns URL resolution.

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

ace_streaming_channels (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL
)
```

`redirect_channels` is the only table used by the current handlers.
`ace_streaming_channels` is a leftover from the removed ACE streaming design.
The post-deploy hook still checks that it exists; that check goes away with the
ACE cleanup.

There are **no migrations**. `init_db()` only creates missing tables. If a
column changes, an existing database will not be altered automatically. The
safe procedure is:

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
| data | `/var/lib/ytaceproxy-prod` | `/var/lib/ytaceproxy-dev` |
| backups | `/var/backups/ytaceproxy/prod` | `/var/backups/ytaceproxy/dev` |

Both stacks can run simultaneously. They share no database, cache, manifest or
port. The manager container names follow `ytaceproxy-<env>-manager-1`.

The manager image is built from this repository. The `acestream` and `acexy`
images are pinned in `.github/workflows/release.yml` and resolved to digests by
the `companions` job; they are not built here.

## Lifecycle and failure behaviour

- `docker compose up -d --wait` waits for the manager healthcheck (TCP port
  80) and for running containers.
- `pre-deploy.sh` creates a consistent SQLite backup before the image switch.
  It skips the backup only when the manager is not running or `proxies.db`
  does not exist yet.
- `post-deploy.sh` checks `PRAGMA integrity_check`, the required tables and the
  redirect count. A failure makes the deploy wrapper roll the **image** back
  to `manifest.prev.json`; the database is not rolled back automatically.
- `30_services` reads `/var/lib/deploy/<app>/manifest.json`, writes
  `/srv/<app>/deploy-images.env` and passes it to compose. Without a manifest
  it uses the bootstrap tag from `ytaceproxy_environments`.
- Every service uses the `json-file` log driver with `max-size=10m` and
  `max-file=3`.

## Planned architecture change: acexy redirect

The intended replacement for the old ACE path is a redirect endpoint, for
example:

```text
GET /ace/{name}  →  302 http://<tailnet-ip>:<acexy-port>/ace/getstream?id=<content-id>
```

The manager needs the per-environment acexy base URL (prod `8090`, dev `8091`)
because it is a different published port. The planned implementation adds an
environment variable (for example `ACEXY_BASE_URL`) to the compose template,
looks up the content id in the database and returns a 302. The design has to
decide whether `url` stores a source URL (for yt-dlp) or a content id (for
acexy); do not overload one column silently.

The old `/ace/{channel}` video-streaming implementation is already gone. The
remaining ACE artifacts are the legacy table and a stub; see
[roadmap.md](roadmap.md).
