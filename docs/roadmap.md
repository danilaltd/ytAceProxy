# Roadmap

This file records the next agreed changes. Completed work is removed rather
than kept as a changelog.

Done:

- migrated dependency management and the image build to `uv`;
- removed the ACE playback path entirely: `/ace`, `ace_channels`,
  `ACEXY_BASE_URL` and the ACE admin pages;
- replaced the two tables with one `channels` table in a new
  `/app/data/channels.db` (clean start, no migration; old `proxies.db` removed
  per environment);
- renamed the code-level project to `playgate`; `/play/{name}` is the only
  public redirect, `/admin/channels` is the only admin page;
- added per-channel quality (`channels.format`), TTL badges, per-row
  “Update now”, a local stylesheet and a unified admin;
- added `/health` and switched the manager healthcheck to it;
- tightened the post-deploy hook to require `channels`;
- moved the server compose into `deploy/compose/` and switched both stacks to
  manifest v2 and wrapper v2;
- added a CI `test` job (`uv sync --frozen` + `uv run pytest`) that gates the
  build, and resolver/schema/form regression tests after the 2026-11-20
  incident.

Remaining work:

## 1. Structured logging

Add structured logging with request/channel fields. Source and resolved URLs
must stay out of the logs (they may carry tokens); this is already enforced in
the resolver, keep it that way.

## 2. Decide the fate of the acestream/acexy companions

The `/ace` route is gone and nothing in the manager references these services,
but the compose still ships and publishes them (host ports `6878/6879` and
`8090/8091`). Options:

1. move `acestream` + `acexy` to a host-level Docker compose managed by
   Ansible (leaning, **deferred, not decided**);
2. remove them from the stack entirely;
3. keep them as they are.

Any option changes the exact manifest image set, the wrapper allowlist, the
compose policy, `roles/verify`, `exposure.yml` and `.env`. The infrastructure
rename of the deployed identities (`ytaceproxy-*`, image, `/srv`, role)
should happen together with this decision, not before, to avoid two cutovers.

## 3. Memory limits

Revisit memory limits after a week of `docker stats` data. Current usage is far
below the host limit, so this is not urgent.

## Order

1. Structured logging.
2. Companions decision (and the deferred infrastructure rename with it).
3. Memory limits.

Each application change follows the release path: `dev` → verify → `main` →
verify, with a pre-deploy backup and a green `40_verify`. Compose changes go
through `deploy/compose/docker-compose.yml`, not an Ansible template.
