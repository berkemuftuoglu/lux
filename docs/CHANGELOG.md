# Lux Changelog

## 2026-08-29 — frontend type-checks clean, first frontend tests, pipeline cleanup

### Fixed

- **The frontend had never type-checked: `svelte-check` reported 56 errors.** 50 came from
  one cause — shadcn-svelte generates components importing `WithElementRef` and
  `WithoutChildrenOrChild` from `$lib/utils`, which were never added. Now 0 errors.
- **`double precision` columns were not right-aligned in the grid.** `cellClass` compared
  `t === 'double'`, but Postgres reports `double precision`, so the branch never fired and
  those columns rendered left-aligned like text. Caught by the first component test.
- **SqlEditor read fields the backend never sends.** It branched on `affected_rows` and
  `message`; `/api/sql` returns only `row_count`/`columns`/`rows`. Dead code that silently
  denied the user any feedback after a non-SELECT. See TODO — needs a backend change to
  report the command tag's row count.
- `app.svelte.ts` assigned `number | undefined` to a `number | null` field; DataGrid used
  bits-ui v1's `asChild` alongside v2's `child` snippet.

### Added

- **Vitest + @testing-library/svelte.** 15 tests, ~900 ms: `parseConnectionString`
  round-trips, `ApiError` status/message propagation and non-JSON fallback, and the
  cell-formatting rules. Previously the frontend had zero tests.
- `$lib/format.ts` — `cellClass` extracted from DataGrid so the type-to-alignment rules
  are testable without standing up the grid.
- CI now runs all three Zig gates (it previously skipped `zig build lint`, so lint
  failures only surfaced at tag time) plus a frontend job doing `svelte-check` and vitest.
- Zig cache + pnpm store caching in CI.

### Removed

- **libpq installation from both pipelines.** lux links no C libraries — `@cImport` and
  `linkSystemLibrary` both return zero matches — but CI installed `libpq-dev`, and the
  release matrix ran `brew install libpq` and `choco install postgresql16`, installing a
  whole database server on the Windows runner for a library nothing uses.

### Note for maintainers

`frontend/build` does **not** reach the binary on its own. After changing the frontend:
`cd frontend && pnpm build && node bundle.cjs` — bundle.cjs copies the output into
`src/static/` and regenerates `src/static_files.zig`, which hardcodes Vite's
content-hashed filenames. Skipping it either embeds a stale UI or fails the build with
`unable to open 'static/_app/env.js'`.

## 2026-08-29 — honest startup, and a busy port no longer lies

### Fixed

- **Startup announced a URL it was not serving.** `serve()` logged
  "Lux web UI running at http://…" before `server.listen()` bound the socket, so a busy
  port printed a success banner, then leaked two internal errors
  (`Cannot stop server, .listen() was never called`, then a raw `error: AddressInUse`
  stack trace). `listen()` failure is now reported against the URL that was advertised,
  and a busy port exits cleanly with an actionable message instead of a Zig trace.

### Added

- `findAvailablePort` probes upward from the requested port before anything is announced.
- A port given explicitly with `--port` is never silently moved — moving it would break
  the caller's bookmarks and scripts — so a collision is a clear error. The **default**
  port advances up to 20 slots and says so loudly:
  `port 8080 was busy — using 8081 instead`.

## 2026-08-29 — thread-safety for shared server state

httpz runs handlers on a 32-thread pool by default (`config.threadPoolCount()`), and
`serve()` passes no thread_pool config. `ServerState` had exactly one mutex, `ws_mutex`,
guarding `ws_clients` alone — every other shared field was mutated from worker threads
with no synchronisation.

### Fixed

- **Use-after-free in journal undo.** `handleJournalUndo` held a raw `*ChangeEntry` into
  `change_journal.items` across escaping, SQL building and a blocking round-trip. A
  concurrent edit could realloc the array or, at `max_journal_entries`, evict entry 0 and
  free its seven strings — feeding recycled heap into `escapeIdentifier` and out as a
  quoted SQL identifier. Callers now take an owned `ServerState.JournalSnapshot`.

- **Use-after-free of the schema arenas.** `freeSchemaState` deinits both arenas (and the
  pool) on connect/reconnect/refresh while `handleTableData` was dereferencing
  `TableInfo.name` and `primary_key_columns[0]` into `ORDER BY "{s}"`.

### Added

- `ServerState.journal_mutex` / `history_mutex` (mixed read/write, short sections) and
  `schema_lock: std.Thread.RwLock` (read on every request, written only on connect and
  refresh — the shared-lock pattern from cache.zig's segments).
- `appendJournalEntry`, `snapshotJournalEntry`, `markJournalUndone`, `clearJournal` — the
  journal is no longer mutated directly by handlers.
- Reader handlers in crud/export/sql take `schema_lock.lockShared()` at entry;
  connect/reconnect/refresh take it exclusively around the swap.

Note: `freeSchemaState` deliberately does not take the lock — std.Thread.RwLock is not
reentrant and callers already hold it.

### Verified

Three gates exit 0. Live stress against Postgres: 100 concurrent journal undos (each a
real DB round-trip) against 61 concurrent edits and 80 schema refreshes — all 200s, no
crash, row count intact.

## 2026-08-29 — jsonb read corruption + actionable UPDATE errors

### Fixed

- **jsonb values were corrupted on every read** (`src/lib/pg_decode.zig`). `decodeJsonb`
  unconditionally stripped the first byte, assuming the binary wire format's 1-byte
  version prefix (0x01). Values arriving over the text protocol have no such prefix, so
  the opening `{` was eaten: `{"ok": true}` displayed as `"ok": true}`. Editing any jsonb
  cell then failed, because the grid faithfully sent the corrupted string back and
  PostgreSQL rejected it with `invalid input syntax for type json`.
  Now the prefix is stripped only when present; 0x01 is not valid as the first byte of
  JSON text, so the check is unambiguous.

  The existing test asserted the buggy behaviour (synthetic binary input only), which is
  why 118 passing tests did not catch it. Replaced with three tests covering binary form,
  text form, and non-object JSON values (arrays, strings, numbers, null).

- **UPDATE failures now report PostgreSQL's own message.** `runQueryOnConnParams` logged
  the server's message and then discarded it behind `error.QueryFailed`, so every failed
  edit surfaced as a bare `{"error":"Update query failed"}` with nothing actionable.
  Added `postgres.runQueryParamsReporting`, which copies the message into a caller-supplied
  slot, and `/api/update` now returns it as a `detail` field.

## 2026-05-17 — v0.1.0 — First public release

**Lux v0.1.0 is the first cut.** A lightweight, local-first PostgreSQL web client. Single binary, zero install, zero cloud. UI compiled into one ~1.4 MB Zig executable. No Electron, no JVM, no Python.

### What's included

- Connect to PostgreSQL (saved connections, named, color-coded)
- Browse schema (public schema; table tree with type chips and constraint badges)
- View / edit / insert / delete rows (per-cell inline edit with a pending-edit journal)
- SQL editor with autocomplete (CodeMirror 6 — tables, columns, keywords, functions) and multi-statement script support
- Find & Replace with preview-then-commit
- Export CSV / JSON (both table data and SQL results)
- Import CSV (RFC 4180 parser, transactional — all-or-nothing)
- Command palette (`Ctrl+K`) — search tables, columns, actions
- Read-only mode (whitelist-based — even `EXPLAIN ANALYZE DELETE` is blocked)
- Dark / light themes (WCAG AA contrast)
- Single binary (~1.4 MB ReleaseSmall — embeds the Svelte UI via `@embedFile`)
- Cross-platform binaries: Linux x86_64 / ARM64, macOS Intel / Apple Silicon, Windows x86_64

### Known limitations (be honest)

- **Public schema only.** Non-public schemas work via SQL `SET search_path = my_schema, public;` — full multi-schema UI deferred to v2.0 (see BACKLOG.md).
- **No ALTER TABLE UI.** Use the SQL editor — `ALTER TABLE ... ADD COLUMN ...` works fine.
- **No role/user management.** Use `psql` or `\du`.
- **No post-commit undo.** Once a transaction commits, Postgres has discarded the rollback data. The "Discard pending changes" button reverts unsaved per-cell edits only — this is honest about what's possible. Use point-in-time recovery for real undo.
- **No LISTEN/NOTIFY live updates.** Manual refresh — deferred to v0.2.0+.
- **x86_64-linux is the most-tested target.** macOS and Windows binaries cross-compile cleanly and are built by CI, but real-hardware smoke testing is pending. Bug reports welcome.
- **Unsigned binaries** on macOS and Windows. See README "First-run friction" — `xattr -d com.apple.quarantine ./lux` (macOS) or **More info → Run anyway** (Windows). Code signing is on the v0.2.0+ roadmap.
- **Playwright e2e suite** (388 CUJs) needs selector retrofit for the Svelte DOM — functional but not yet wired against the new frontend. Backend Zig test suite (401 tests) is fully green.

### Verified-working since the v1.0 audit

- CSRF protection (Origin-header check on POST/DELETE)
- Content-Security-Policy header on HTML responses
- SQL injection hardening (`escapeIdentifier` rejects null bytes; `escapeStringValue` doubles `'` and `\`)
- Read-only enforcement on every SQL-accepting endpoint (whitelist, not blacklist)
- `connections.json` stored in `$XDG_CONFIG_HOME/lux/` with mode `0600` (owner-only)
- pg.zig native Zig driver replaces the hand-rolled libpq C FFI (Phase 13) — pool-per-handler, parameterized queries throughout
- pg.zig `.cache_name` on hot schema queries (Phase 16) — prepared-statement reuse on every connect + Refresh
- Svelte 5 + shadcn-svelte + Tailwind 4 frontend rewrite (Phase 14 → 15), bundled to `src/static_files.zig` and embedded via `@embedFile`
- pg.zig binary→text decoding for ~20 PostgreSQL types (`src/lib/pg_decode.zig`)
- Stack-buffer use-after-return class eliminated with a build-step grep guard
- 401+ Zig tests, all three build gates green (`zig build test` + `zig build -Doptimize=ReleaseSafe` + `zig build lint`)

### Distribution

Binaries are built and published by `.github/workflows/release.yml`, which runs all three build gates as a precondition, then cross-compiles 5 targets via Zig and uploads them to the GitHub Release.

```
lux-v0.1.0-linux-x86_64
lux-v0.1.0-linux-aarch64
lux-v0.1.0-macos-x86_64
lux-v0.1.0-macos-aarch64
lux-v0.1.0-windows-x86_64.exe
```

### Tagging this release (user runs these — per CLAUDE.md the agent never touches git)

```bash
# Precondition — all four gates green:
zig build test
zig build -Doptimize=ReleaseSafe
zig build lint
cd e2e && npm test   # Playwright smoke — see Known Limitations note above

# Then tag and push (this triggers release.yml):
git tag -a v0.1.0 -m "Lux v0.1.0 — first public release"
git push origin v0.1.0
```

Once the tag lands, watch the workflow on the GitHub Actions page. When all 5 matrix builds + the publish job succeed, the binaries are live on the [Releases page](https://github.com/berkemuftuoglu/lux/releases/latest).

---

## 2026-05-17 — v1.2 Release Hardening: Svelte migration complete

The headline change: the frontend is now Svelte. The HTTP/data layer is now pg.zig.

### Frontend rewrite (Phase 14 → Phase 15)
- Replaced ~10K lines of vanilla JS / CSS / HTML with **Svelte 5 (runes)** + **shadcn-svelte** + **Tailwind CSS 4** + **CodeMirror 6** (SQL editor).
- 14 panel components: Sidebar, DataGrid, SqlEditor, ConnectDialog, StatusBar, CommandPalette, WelcomeScreen, HistoryPanel, JournalPanel, SavedConnections, CreateTableDialog, ImportCsvDialog, InsertRowDialog, TableInfoPanel.
- Build pipeline: `cd frontend && npm run build && node bundle.cjs` regenerates `src/static_files.zig` for the single-binary `@embedFile` embed.
- ReleaseSafe binary: 33 MB. ReleaseSmall binary: 1.4 MB. Same pitch (single binary, zero install) — accurate now.

### Backend modernization (Phase 13)
- Replaced hand-rolled libpq C FFI with **karlseguin/pg.zig** native Zig driver. Eliminates `@cImport`, removes `libpq.so` runtime dependency, gains connection pooling.
- Connection-per-request → pool-per-handler (`pool.acquire()` / `conn.release()`).
- Parameterized queries throughout (replaces interpolation in CRUD handlers).
- Pool-wide `SET default_transaction_read_only` on read-only mode toggle.
- 7 Zig dependencies wired into production: zul, zig-clap, websocket, cache (schema TTL), log (logz access logs), validate (CRUD validation), pg.zig.

### pg.zig binary-protocol decoding (Plan 15-01)
- Added `src/lib/pg_decode.zig` (~20 PostgreSQL type decoders) — int2/4/8, float4/8, bool, text/varchar/bpchar, numeric, timestamp/timestamptz/date/time, uuid, bytea, json/jsonb, inet/cidr.
- Returns text representation matching libpq's default (no contract change for the frontend / CSV export / SQL editor).
- 22 byte-level test cases covering edge cases (NaN, truncated input, unknown OIDs).
- Wired into `src/server/postgres.zig:extractResult` via `decodeRow` — affects every JSON row response (`/api/tables/.../data`, `/api/sql`).

### Stack-buffer bug class eliminated (Plans 15-01 + 15-02)
- Caught 3 handlers using `var resp_buf: [N]u8 = undefined; bufPrint(&resp_buf, ...)` and passing the slice to `sendJsonResponse` after the function returned — leaving httpz with a dangling pointer (`handleHealthCheck`, `handleReadOnlyGet`, `handleReadOnlyToggle`).
- Fixed by switching to `std.fmt.allocPrint(res.arena, ...)` or string literals.
- Added a build-step grep guard (`zig build lint`) that fails the build if the pattern reappears in `src/handlers/`.
- Top-of-file comment in `src/handlers/schema.zig` documents the rule for future contributors.

### Frontend/backend API contract alignment (Plan 15-02)
- `frontend/src/lib/api/client.ts` rewritten to match the literal JSON keys each Zig handler emits/expects. Fixed 5 mismatch points:
  - `/api/tables/.../data`: read `total/limit/offset` (was `total_rows/page_size/has_more`)
  - `/api/health`: read `connected` (was `status`)
  - `/api/connections`: parse `conninfo` URI client-side via `parseConnectionString` helper (was reading separate host/port/database/user fields the backend never returned)
  - `/api/settings/read-only`: backend now accepts both legacy `{enabled:"true"}` and modern `{read_only:bool}` shapes
  - `/api/journal/undo`: send `{id: string}` (was `{index: number}`)

### CRUD + Export/Import wiring (Plans 15-03 + 15-04)
- DataGrid: cell edit (`saveEdit` awaits `api.updateCell` then refreshes — no optimistic UI), delete-row with confirm, set-NULL via context menu, find-and-replace with preview-then-commit.
- InsertRowDialog: per-column inputs with type chips; column defaults shown as **placeholders** (not pre-filled values) so PG defaults like `nextval(...)` and `now()` apply automatically.
- `handleInsertRow` now returns **HTTP 500** on query failure (was silently 200 with error body — caused fake success toasts on failed inserts).
- JournalPanel: UI label renamed `Undo` → `Discard pending changes` (per scope-discipline brief — post-commit undo isn't safely possible; `api.undoJournal` function name preserved).
- Connect dialog: added Save-and-Connect with name + 6-color palette (Wave 4 scope creep that fit cleanly).

### Scope cuts (per 2026-05-17 brief)
- ER diagrams: cut entirely (no Svelte component exists; vanilla JS was deleted in commit `2327a80`).
- mecha dependency: removed from `build.zig` + `build.zig.zon` (declared but unused — dead weight).
- Schema-beyond-public, ALTER TABLE UI, role management, LISTEN/NOTIFY, true post-commit undo: all moved to `BACKLOG.md`.

### QA against real Postgres
- HTTP smoke against lux-pg Docker container: all 5 endpoints return decoded text (no raw bytes). Stack-buffer regression guards hold.
- Browser QA in Chrome: 11/22 panels fully verified, 1 partial (sort indicator), 2 bugs found and fixed inline during QA, 8 untested but low-risk (same wiring as verified panels).
- See `.planning/phases/15-server-side-html-rendering/15-QA-CHECKLIST.md` for the full matrix.

### Known limitations (be honest)
- `total` row count is briefly stale after insert (schema cache TTL — fix is ~2 lines, deferred to v1.3)
- Column-header sort doesn't show a visual indicator yet (functional, no affordance)
- Playwright suite (388 CUJs from v1.1) needs selector retrofit for Svelte DOM — deferred to v1.3
- Public schema only, no ALTER TABLE UI, no role management — by design, see `BACKLOG.md`
- Most-tested target is x86_64-linux; macOS/Windows pending Phase 16 cross-compile

### Build gates (all green at ship time)
- `zig build test` — 401 tests pass
- `zig build -Doptimize=ReleaseSafe` — exit 0
- `zig build lint` — 0 errors, 6 unrelated postgres.zig warnings
- `cd frontend && npm run build` — exit 0

### Suggested commit (user controls git per CLAUDE.md)
```
feat(v1.2): complete Svelte migration + pg.zig native driver + stack-buffer fixes

Phase 13: pg.zig replaces libpq C FFI
Phase 14: Stitch UI parity (vanilla, since superseded)
Phase 15: Svelte 5 rewrite — pg.zig binary decoding, API contract alignment,
         CRUD wiring, journal "Discard" label rename, browser QA against lux-pg.

See docs/CHANGELOG.md for the full summary.
```

---

(Pre-v1.2 changes lived in the git log only; this file starts at v1.2.)
