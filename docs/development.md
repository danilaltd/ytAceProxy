# Development

## Layout

| Path | Purpose |
|---|---|
| `proxy_ace/app.py` | `aiohttp` application, routes, startup/shutdown |
| `proxy_ace/handler.py` | `/yt_dlp`, `/ace` handlers and redirect response |
| `proxy_ace/admin_routes.py` | admin list/add/edit/delete/sync routes |
| `proxy_ace/config.py` | `yt-dlp` resolution, sync logic, TTL parsing |
| `proxy_ace/ace.py` | acexy URL construction from `ACEXY_BASE_URL` |
| `proxy_ace/db.py` | SQLite path and schema creation |
| `proxy_ace/repo.py` | SQL queries for redirects and ACE channels |
| `proxy_ace/models.py` | `RedirectChannel` and `AceChannel` dataclasses |
| `proxy_ace/state.py` | process-wide stop event |
| `proxy_ace/forms/` | WTForms and Jinja2 templates |
| `tests/` | pytest unit and handler tests |
| `pyproject.toml`, `uv.lock` | dependencies and lock file |
| `Dockerfile` | image build with `uv sync --frozen` |
| `.github/workflows/release.yml` | build and digest deploy |

There is no `channels.json` and no watcher. The old `/ace/{channel}` streaming
endpoint is gone; `/ace/{name}` is now a redirect into `acexy`.

## Run locally

Dependencies are managed by `uv`:

```bash
uv sync --frozen
ACEXY_BASE_URL=http://localhost:8090 uv run python -m proxy_ace.app
uv run pytest
```

The server binds `0.0.0.0:80` (through `web.TCPSite(port=80)`), so either run
with the needed privileges or temporarily change `LISTEN_PORT` in
`proxy_ace/app.py` on a local branch.

The database path is the container path `/app/data/proxies.db`. For a
non-Docker run, mount or create that directory, or change `DB_PATH` in a local
patch. Do not commit a local path change.

`ACEXY_BASE_URL` is read from the environment at request time. A local value is
`http://localhost:8090` when the browser runs on the Docker host; use the host
IP when the browser is elsewhere. Without it, `/ace` returns 503.

## Add or change a redirect route

1. Add the handler in `proxy_ace/handler.py`.
2. Register it in `create_app()` in `proxy_ace/app.py`.
3. If it reads the database, add a function in `proxy_ace/repo.py` instead of
   opening `aiosqlite` in the handler.
4. If it changes a redirect, mark the model `dirty` and persist through
   `update_redirects()`.
5. Add or update tests in `tests/`.
6. Update `README.md`, `docs/architecture.md` and the deployment checklist if
   the public contract changes.
7. Push `dev`, verify `/yt_dlp/...` or `/ace/...`, then merge `main`.

Keep the unknown-channel contract: `/yt_dlp`: channel → `placeholder` → 404.
`/ace`: unknown name → 404; no placeholder fallback.

`/ace` must return a client-reachable URL. Do not use the Docker service name
`acexy` as the base URL; clients do not resolve it. Use
`ace.build_acexy_stream_url()` instead of concatenating strings.

## Add a database column or table

The schema is created only with `CREATE TABLE IF NOT EXISTS`; there is no
migration runner. A new table works automatically for a brand-new database and
for an existing one on the next startup. A new column does not.

1. Change `CREATE_TABLES_SQL` in `proxy_ace/db.py`.
2. Update `proxy_ace/repo.py` and the model.
3. If the post-deploy hook must check the new table, update
   `arch2debian/roles/ytaceproxy/templates/post-deploy.sh.j2` and apply it
   with `31_apps -e env=dev|prod`.
4. Take a pre-deploy backup and test on `dev` first.
5. If the change cannot be additive, write the one-off migration for the
   existing database and document it in `docs/deployment.md`.

Never mutate `proxies.db` on the host by hand while the manager is running.
Use the pre-deploy backup or a one-shot `docker exec` with `sqlite3`, and keep
the result in the deployment log.

`ace_channels` was added additively. The old `ace_streaming_channels` is no
longer created and was dropped on dev and prod. The post-deploy hook still
accepts it during the transition; see [roadmap.md](roadmap.md).

## Resolver notes

`config.py` runs `yt-dlp` in a separate process because extraction is
synchronous and can block for seconds. When changing it:

- keep the timeout (default 60 s) and the process cleanup in `finally`;
- keep `MAX_PROCESSES` bounded;
- do not call `YoutubeDL` from the event loop;
- remember `ttl = -1` means “eternal” and a future timestamp means “valid”;
- log failures without logging source credentials or query secrets.

ACE channels do not use the resolver: they are a name plus a content id and are
served directly as a redirect.

## Tests

Tests use `pytest` + `pytest-aiohttp` and do not need the network:

```bash
uv run pytest
```

- `tests/test_ace_url.py` covers URL construction, trailing slashes, encoding,
  and invalid or missing `ACEXY_BASE_URL`.
- `tests/test_ace_handler.py` uses a temporary SQLite file and the aiohttp test
  client to cover 302, 404 and 503, plus the fresh-database table set.

Do not hit a real streaming site in tests. When adding a test for the resolver,
use a fake resolver or a pure function.

## Dependency management (uv)

`pyproject.toml` is the source of dependencies; `uv.lock` is committed. The
image uses `uv sync --frozen --no-dev`, so an out-of-date lock fails the build.

```bash
uv add <package>          # runtime
uv add --dev <package>    # tests/tooling
uv lock                   # after editing pyproject.toml by hand
uv sync --frozen
```

Rules:

- do not edit `uv.lock` by hand;
- keep the Python version at `3.11` (`.python-version`) unless a separate
  change updates the image and is tested;
- do not add `requirements.txt` back; `uv export` is only a last-resort
  compatibility export;
- keep `/app/data`, port 80 and the image name unchanged.

## Commit style

One concern per commit, message `type(scope): summary` (for example
`feat(ace): redirect /ace to acexy`, `test(ace): cover URL construction`).
Do not rewrite history or move tags.
