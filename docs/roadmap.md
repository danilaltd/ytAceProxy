# Roadmap

This file records the next agreed changes. They are **not implemented yet**.
Do each one in a dedicated branch/commit, push `dev`, verify, then merge
`main`. Do not mix them with unrelated fixes.

## 1. Remove the remaining ACE artifacts

### Current state

The old `/ace/{channel}` streaming endpoint is already gone. The remaining
ACE-specific pieces are:

- `ace_streaming_channels` in `proxy_ace/db.py` (created on every new database);
- `ace_streaming_channels` in the post-deploy required-table check
  (`arch2debian/roles/ytaceproxy/templates/post-deploy.sh.j2`);
- `load_channels()` in `proxy_ace/config.py` returns a stub with an `"ace"`
  key and is not called;
- references to the old design in docs.

### Steps

1. Remove `load_channels()` and the `"ace"` branch from `config.py`.
2. Stop creating `ace_streaming_channels` in `db.py`. Existing databases keep
   the orphan table; that is harmless until an explicit cleanup.
3. Update `post-deploy.sh.j2` to require only the tables the code actually
   uses.
4. Update `docs/architecture.md`, `docs/development.md` and the README data
   model.
5. Optional maintenance cleanup for an existing database, only after a backup:

   ```bash
   ssh asus@debian "sudo /srv/ytaceproxy-dev/pre-deploy.sh pre-ace-cleanup"
   ssh asus@debian "sudo docker exec -i ytaceproxy-dev-manager-1 python - <<'PY'
   import sqlite3
   c = sqlite3.connect('/app/data/proxies.db')
   c.execute('DROP TABLE IF EXISTS ace_streaming_channels')
   c.commit()
   c.close()
   PY"
   ```

   Do this for `dev` first, then `prod`, and record it in the deployment log.
6. Verify `post-deploy.sh` and `/admin/redirects` on dev.

### Acceptance

- no code path reads or writes `ace_streaming_channels`;
- a fresh database has only the active tables;
- `post-deploy.sh` passes;
- existing redirects are unchanged.

## 2. Switch playback to acexy with a convenient redirect

### Target behaviour

The manager should expose a short URL that redirects the client to the
per-environment acexy service:

```text
GET /ace/{name}
  → 302 http://<tailnet-ip>:<acexy-port>/ace/getstream?id=<content-id>
```

`name` is a stable channel name; `content-id` is the AceStream content id/hash.
The redirect is performed by the manager, but the media connection is between
the client and acexy/acestream.

### Design decisions to settle before coding

1. **Where the content id lives.** Do not overload
   `redirect_channels.url` (that is a yt-dlp source URL). Prefer a separate
   table, for example:

   ```sql
   ace_channels (
       id INTEGER PRIMARY KEY,
       name TEXT UNIQUE NOT NULL,
       content_id TEXT NOT NULL,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   )
   ```

   Alternatively add `kind`/`target` columns to `redirect_channels`, but then
   every existing query must filter by kind. The separate table is simpler and
   keeps the current `/yt_dlp` contract untouched.
2. **How the manager learns the acexy address.** The client follows the
   redirect, so `http://acexy:80` (Docker DNS) is not reachable from the
   client. Add an Ansible-rendered environment variable, for example
   `ACEXY_BASE_URL=http://<tailnet-ip>:8090` for prod and
   `...:8091` for dev. The value belongs in the compose template, not in the
   image.
3. **Fallback.** Decide whether `/ace/{name}` falls back to a `placeholder`
   ace channel, returns 404, or returns 302 to an error page. Keep the rule
   explicit and documented.
4. **Admin UI.** Add a small ace-channel form (name + content id) or extend the
   redirect form with a type. Do not make users paste a full URL when a content
   id is enough.
5. **URL encoding.** Use `urllib.parse.urlencode`/`quote` for the content id
   and the base URL; do not concatenate raw user input.

### Steps

1. Add the table and repository functions.
2. Add `GET /ace/{name}` in `handler.py`, register it in `app.py`.
3. Add `ACEXY_BASE_URL` to the Ansible compose template and the environment
   defaults; run `30_services` plus `40_verify`.
4. Add admin UI fields/routes if the table is managed there.
5. Add unit tests for URL construction, fallback and missing content id.
6. Update README, `docs/architecture.md`, `docs/deployment.md` and
   `docs/troubleshooting.md`.
7. Remove the old ACE table in the same release or a follow-up, after the new
   path is proven on dev.

### Acceptance

- `/ace/<name>` returns 302 to the correct acexy port for the environment;
- `/yt_dlp/<name>` is unchanged;
- 404/fallback behaviour is tested;
- prod and dev redirect to their own acexy ports;
- no content id appears in logs.

## 3. Migrate to uv

Do this as a dedicated change, with no behaviour change.

1. Add `pyproject.toml` with the five runtime dependencies and optional
   `dev` group.
2. Generate and commit `uv.lock`; pin `.python-version`.
3. Update `Dockerfile` to install from the lock with
   `uv sync --frozen --no-dev` and keep `/app/data` and port 80.
4. Update local commands in `README.md`, `AGENTS.md` and
   `docs/development.md`.
5. Keep `requirements.txt` only as a generated compatibility export
   (`uv export`) if another tool still needs it; remove it once nothing does.
6. Push `dev`, build the image, run `/admin/redirects` and a test redirect,
   then merge `main`.

The detailed checklist is in [development.md](development.md#uv-migration).

### Acceptance

- `uv sync --frozen` works from a clean checkout;
- the Docker image builds without pip resolving unpinned packages;
- the running image is unchanged in behaviour, port and data path;
- CI deploy on dev is green before `main`.

## 4. Small follow-ups

- Add a `/health` endpoint that checks SQLite read access; use it in the
  manager healthcheck if it is more useful than the current TCP connect.
- Add unit tests for TTL extraction and fallback selection.
- Add structured logging with request/channel fields, without logging resolved
  URLs that contain credentials.
- Revisit memory limits after a week of `docker stats` data.

## Order

1. uv migration (build-only, lowest behaviour risk).
2. ACE cleanup (remove dead code/table).
3. acexy redirect (new feature, depends on the cleanup decision).
4. Tests/health endpoint.

Each step follows the same release path: `dev` → verify → `main` → verify,
with a pre-deploy SQLite backup and a green `40_verify`.
