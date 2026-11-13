# Development

## Layout

| Path | Purpose |
|---|---|
| `proxy_ace/app.py` | `aiohttp` application, routes, startup/shutdown |
| `proxy_ace/handler.py` | `/yt_dlp` handlers and redirect response |
| `proxy_ace/admin_routes.py` | admin list/add/edit/delete/sync routes |
| `proxy_ace/config.py` | `yt-dlp` resolution, sync logic, TTL parsing |
| `proxy_ace/db.py` | SQLite path and schema creation |
| `proxy_ace/repo.py` | SQL queries for redirects |
| `proxy_ace/models.py` | `RedirectChannel` dataclass |
| `proxy_ace/state.py` | process-wide stop event |
| `proxy_ace/forms/` | WTForms and Jinja2 templates |
| `Dockerfile` | image build |
| `.github/workflows/release.yml` | build and digest deploy |

There is no `channels.json`, no watcher and no producer loop. The old
`/ace/{channel}` streaming endpoint is gone.

## Run locally

Current flow (pip + `requirements.txt`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m proxy_ace.app
```

The server binds `0.0.0.0:80` (through `web.TCPSite(port=80)`), so either run
with the needed privileges or temporarily change `LISTEN_PORT` in
`proxy_ace/app.py` on a local branch.

The database path is the container path `/app/data/proxies.db`. For a
non-Docker run, mount or create that directory, or change `DB_PATH` in a local
patch. Do not commit a local path change.

Planned uv flow:

```bash
uv sync
uv run python -m proxy_ace.app
```

## Add or change a redirect route

1. Add the handler in `proxy_ace/handler.py`.
2. Register it in `create_app()` in `proxy_ace/app.py`.
3. If it reads the database, add a function in `proxy_ace/repo.py` instead of
   opening `aiosqlite` in the handler.
4. If it changes a redirect, mark the model `dirty` and persist through
   `update_redirects()`.
5. Update `README.md`, `docs/architecture.md` and the deployment checklist if
   the public contract changes.
6. Push `dev`, verify `/yt_dlp/...`, then merge `main`.

Keep the unknown-channel contract: channel → `placeholder` → 404.

## Add a database column or table

The schema is created only with `CREATE TABLE IF NOT EXISTS`; there is no
migration runner. A new column works automatically for a brand-new database
but not for an existing one.

1. Change `CREATE_TABLES_SQL` in `proxy_ace/db.py`.
2. Update `proxy_ace/repo.py` and the model.
3. If the post-deploy hook must check the new table, update
   `arch2debian/roles/ytaceproxy/templates/post-deploy.sh.j2`.
4. Take a pre-deploy backup and test on `dev` first.
5. If the change cannot be additive, write the one-off migration for the
   existing database and document it in `docs/deployment.md`.

Never mutate `proxies.db` on the host by hand while the manager is running.
Use the pre-deploy backup or a one-shot `docker exec` with `sqlite3`, and keep
the result in the deployment log.

## Resolver notes

`config.py` runs `yt-dlp` in a separate process because extraction is
synchronous and can block for seconds. When changing it:

- keep the timeout (default 60 s) and the process cleanup in `finally`;
- keep `MAX_PROCESSES` bounded;
- do not call `YoutubeDL` from the event loop;
- remember `ttl = -1` means “eternal” and a future timestamp means “valid”;
- log failures without logging source credentials or query secrets.

## Tests

There is currently no automated test suite. `test.py` is a manual
`yt-dlp` probe against a fixed URL and is excluded from the Docker context.

Before adding tests, prefer small unit tests for pure functions
(`ttl` extraction, fallback selection, URL construction). A future test run
should use a temporary SQLite file and a fake resolver; no test should hit a
real streaming site or require the network.

## uv migration

This is planned; do it in a dedicated change, not together with behaviour
changes.

### Files

- `pyproject.toml` — project metadata, Python requirement, runtime deps,
  optional `dev` group (pytest, ruff).
- `uv.lock` — committed lock file.
- `.python-version` — pin the interpreter used locally and in CI.
- `Dockerfile` — install from the lock with `uv sync --frozen --no-dev`.
- `requirements.txt` — either remove after the migration or keep as a generated
  compatibility export (`uv export`), but never edit it by hand afterwards.

### Suggested Dockerfile shape

```dockerfile
FROM python:3.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY . ./
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 80
CMD ["python", "-m", "proxy_ace.app"]
```

Pin the uv image by digest or a fixed version in a real change; `latest` above
is only illustrative. Keep `/app/data` and port 80 unchanged so the Ansible
compose and healthcheck do not change.

### Migration checklist

1. Create `pyproject.toml` with the five current runtime dependencies.
2. `uv lock`; commit `uv.lock` and `.python-version`.
3. `uv sync --frozen` locally; run the app and the admin UI smoke check.
4. Update the Dockerfile; build and run the image locally.
5. Update `README.md`, `AGENTS.md`, `docs/development.md` and
   `docs/deployment.md`.
6. Push `dev`; verify the image and `/admin/redirects`.
7. Merge `main` only after the dev check.
8. Remove `requirements.txt` in a follow-up commit if nothing consumes it.

Do not change the image name, port, database path or manifest keys as part of
the uv migration; those are deployment contracts.

## Commit style

One concern per commit, message `type(scope): summary` (for example
`feat(redirect): add acexy target`, `docs(deploy): document uv migration`).
Do not rewrite history or move tags.
