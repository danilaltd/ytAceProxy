# Documentation map

| Document | Holds |
|---|---|
| [../README.md](../README.md) | one-page overview, endpoints, local run, deploy summary |
| [../AGENTS.md](../AGENTS.md) | project map, invariants, change map, agent rules |
| [architecture.md](architecture.md) | components, request flow, resolver, data model, deployment topology |
| [deployment.md](deployment.md) | environments, release flow, what to change where, verification, rollback |
| [development.md](development.md) | local development, adding routes/schema, uv migration |
| [troubleshooting.md](troubleshooting.md) | real failures and the commands that diagnose them |
| [roadmap.md](roadmap.md) | ACE cleanup, acexy redirect, uv migration |

Server-side documentation lives in the Ansible repository:

- `arch2debian/roles/ytaceproxy/README.md` — two stacks, ports, hooks, data;
- `arch2debian/docs/ci-cd.md` — manifest contract, forced command, allowlist;
- `arch2debian/docs/operations.md` — day-2 changes, dev/prod, dry run;
- `arch2debian/docs/decisions.md` — recorded design decisions.

Rules:

- one topic per file, named by subject;
- a contract change updates the document that states it in the same commit;
- no secrets, tokens or private keys;
- when docs and code disagree, the code wins.
