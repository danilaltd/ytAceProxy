# Development

## Layout

| Path | Purpose |
|---|---|
| `playgate/app.py` | `aiohttp` application, routes, startup/shutdown |
| `playgate/handler.py` | `/play`, `/health` handlers and redirect response |
| `playgate/admin_routes.py` | `/admin/channels` routes (list/add/edit/delete/update/sync) |
| `playgate/config.py` | `yt-dlp` resolution, per-format dedup, sync logic, TTL parsing |
| `playgate/ttl.py` | TTL state and human duration for the admin badge |
| `playgate/db.py` | SQLite path, schema creation, health read |
| `playgate/repo.py` | SQL queries for channels |
| `playgate/models.py` | `Channel` dataclass |
| `playgate/state.py` | process-wide stop event |
| `playgate/forms/` | WTForms and Jinja2 templates (`base`, `channels`, `channel_form`) |
| `playgate/static/admin.css` | local stylesheet, no CDN or build step |
| `tests/` | pytest unit and handler tests |
| `pyproject.toml`, `uv.lock` | dependencies and lock file |
| `Dockerfile` | image build with `uv sync --frozen` |
| `.github/workflows/release.yml` | build, manifest v2 and digest deploy |

There is no `channels.json` and no watcher. ACE playback was removed: there is
no `/ace` route, ACE table or `ACEXY_BASE_URL`.

## Run locally

Dependencies are managed by `uv`:

```bash
uv sync --frozen
uv run python -m playgate.app
uv run pytest
```

The server binds `0.0.0.0:80` (through `web.TCPSite(port=80)`), so either run
with the needed privileges or temporarily change `LISTEN_PORT` in
`playgate/app.py` on a local branch.

The database path is the container path `/app/data/channels.db`. For a
non-Docker run, mount or create that directory, or change `DB_PATH` in a local
patch. Do not commit a local path change.

The admin UI is at `/admin/channels`. Static assets are served from
`playgate/static/` via a package-relative path, so the working directory does
not matter (unlike the old relative template loader).

## Add or change a route

1. Add the handler in `playgate/handler.py`.
2. Register it in `create_app()` in `playgate/app.py`.
3. If it reads the database, add a function in `playgate/repo.py` instead of
   opening `aiosqlite` in the handler.
4. If it changes a channel, mark the model `dirty` and persist through
   `update_channels()`.
5. Add or update tests in `tests/`.
6. Update `README.md`, `docs/architecture.md` and the deployment checklist if
   the public contract changes.
7. Push `dev`, verify `/play/...`, then merge `main`.

Keep the unknown-channel contract: `/play`: channel → `placeholder` → 404.

## Add a database column or table

The schema is created only with `CREATE TABLE IF NOT EXISTS`; there is no
migration runner. A new table works automatically for a brand-new database and
for an existing one on the next startup. A new column does not: existing
databases need a one-shot `ALTER TABLE` after a backup.

1. Change `CREATE_TABLES_SQL` in `playgate/db.py`.
2. Update `playgate/repo.py` and the model.
3. If the post-deploy hook must check the new table, update
   `arch2debian/roles/ytaceproxy/templates/post-deploy.sh.j2` and apply it
   with `31_apps -e env=dev|prod`.
4. Take a pre-deploy backup and test on `dev` first.
5. If the change cannot be additive, document the one-off procedure in
   `docs/deployment.md`.

Never mutate `channels.db` on the host by hand while the manager is running.
Use the pre-deploy backup or a one-shot `docker exec` with `sqlite3`, and keep
the result in the deployment log.

The single-table `/play` model was a clean start: `channels.db` is a new file,
the old `proxies.db` is obsolete, and no data was migrated.

## Resolver notes

`config.py` runs `yt-dlp` in a separate process because extraction is
synchronous and can block for seconds. When changing it:

- keep the timeout (default 60 s) and the process cleanup in `finally`;
- keep `MAX_PROCESSES` bounded;
- do not call `YoutubeDL` from the event loop;
- keep the dedup key `(source URL, format)` — two channels can share a URL with
  different quality;
- reject merge selectors (`+`): the image has no ffmpeg;
- remember `ttl = -1` means “eternal”, a future timestamp means “valid” and
  `NULL` means “unknown”;
- log failures without source/resolved URLs; both can carry tokens. Use the
  `_safe_error()` sanitiser.

## Quality selection

`ChannelForm.format` is a preset select; `ChannelForm.format_custom` is an
optional free-text selector. A non-empty custom value wins and is stored in
`channels.format`; empty means the resolver default. `format_form_data()`
round-trips a stored custom value back into the form.

## Tests

Tests use `pytest` + `pytest-aiohttp` and do not need the network:

```bash
uv run pytest
```

**Testing is mandatory:** `uv run pytest` must pass before every push, and the
release workflow's `test` job gates the build and deploy. A change to the
resolver, database, routes, forms or admin must add or update a test. Do not
validate a resolver change only with a redirect override — that bypasses
resolution.

- `tests/conftest.py` provides a temporary `channels.db` and an `app` fixture;
  an autouse fixture stubs background resolution for admin tests.
- `tests/test_resolver.py` covers the worker contract
  `(url, format, link)`, persistence through
  `update_special_channel()`/`sync_channels()`, dedup by `(url, format)`,
  valid-TTL skips, forced updates and failures.
- `tests/test_db.py` covers the fresh schema and the health read.
- `tests/test_forms.py` covers format mapping, merge-format rejection and
  name validation. Validation tests use a minimal form-data object because
  `Optional()` relies on `raw_data`.
- `tests/test_play.py` covers 302, 404, `placeholder` fallback, expired TTL
  and the `upd` endpoint.
- `tests/test_admin.py` covers list/add/edit/delete/update/sync, duplicate
  names, URL-separator and merge-format rejection, and background resolve
  scheduling.
- `tests/test_ttl.py` covers eternal/valid/expired/unknown and humanisation.
- `tests/test_health.py` covers the 200/503 health responses.

Do not hit a real streaming site in tests. The resolver worker test uses the
`fork` start method (Linux) and is skipped elsewhere.

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
`feat(channels): add quality selection`, `test(ttl): cover the badge states`).
Do not rewrite history or move tags.
