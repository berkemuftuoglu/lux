# Lux — Backlog (Parking Lot)

Items here are intentionally deferred. Per the project's scope discipline rule: "If a feature would take more than 2 weeks or competes with the 'single binary, polished basics' identity, do not build it — move it here."

This file is the cemetery. Items are NOT promises to build later — they are promises to consider before building.

---

## v2.0 Candidates (Reconsider After v1.0 Ships)

### Schema support (browse non-public schemas)
- **Why deferred:** Niche. Most lux users connect to one app database. Multi-schema navigation requires sidebar tree restructuring + qualified identifier handling across every SQL handler. Big lift, narrow audience.
- **Workaround:** Use `SET search_path = my_schema, public;` then queries work.
- **Original source:** TODO.md (moved 2026-05-17 per scope-discipline brief)

### ALTER TABLE UI
- **Why deferred:** Postgres has ~30 ALTER variants (ADD/DROP/RENAME column, type changes, constraints, partitions, defaults, identity, etc.). Building a UI that covers them safely is its own product — full migration tools already exist (Atlas, sqitch, Liquibase, Prisma Migrate).
- **Workaround:** Run ALTER TABLE in the SQL editor. Works fine.
- **Original source:** TODO.md (moved 2026-05-17 per scope-discipline brief)

### User/role management panel
- **Why deferred:** People who care about Postgres role/privilege management use `psql` or `\du`. UI for it duplicates a CLI most DBAs prefer. Not a wedge for the lux audience (devs querying app data).
- **Workaround:** psql, or paste `CREATE ROLE` / `GRANT` into SQL editor.
- **Original source:** TODO.md (moved 2026-05-17 per scope-discipline brief)

### LISTEN/NOTIFY live updates
- **Why deferred:** Cool but a "later wedge," not a "now feature." Adds a WebSocket subscription layer and per-table notify channels. Polish basics + ship binaries first.
- **Workaround:** Manual refresh.
- **Original source:** Scope-discipline brief (2026-05-17)

### True undo across DB ops (post-commit rollback)
- **Why deferred:** Post-commit undo isn't safely possible — once a transaction commits, the engine has discarded the rollback data. TablePlus / Beekeeper / DBeaver don't do this either. The existing "pending edits buffer" (per-cell, pre-commit) IS already implemented; v1.0 just renames the UI label from "Undo" → "Discard pending changes" to set expectations correctly.
- **Workaround:** Use Postgres point-in-time recovery if you really need it; or wrap edits in explicit BEGIN/ROLLBACK.
- **Original source:** Scope-discipline brief (2026-05-17)

### Mobile / responsive layout
- **Why deferred:** Lux is a desktop DB client. Nobody runs production queries from a phone. Adding breakpoints + touch handling is weeks of work for ~0 users.
- **Original source:** PROJECT.md non-goals (always-deferred)

### Multiple database engine support (MySQL, SQLite, etc.)
- **Why deferred:** Each engine has its own wire protocol, type system, and dialect quirks. Becoming pgAdmin-for-all-DBs is a different identity. Stay Postgres-only.
- **Original source:** PROJECT.md non-goals (always-deferred)

### Streaming / lazy grid for huge result sets
- **Why deferred:** LIMIT-based pagination is adequate for the v1 audience. Streaming requires a cursor-based protocol on the backend and virtual scrolling on the frontend.
- **Original source:** PROJECT.md Future
