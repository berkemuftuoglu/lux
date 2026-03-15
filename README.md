<p align="center">
  <img src="docs/logo.svg" alt="Lux logo" width="128" height="128" />
</p>

<h1 align="center">Lux</h1>

<p align="center">
  A lightweight, local-first PostgreSQL web client.
  <br />
  Single binary. Zero cloud. Zero JS frameworks. Your data stays on your machine.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
  <a href="https://ziglang.org"><img src="https://img.shields.io/badge/built%20with-Zig%200.13-f7a41d.svg" alt="Built with Zig" /></a>
</p>

<p align="center">
  <img src="docs/screenshots/screenshot-main.png" alt="Lux table browser showing data grid with schema tree" width="800" />
  <br />
  <em>Browse tables with inline editing, sorting, filtering, and column resizing</em>
</p>

<p align="center">
  <img src="docs/screenshots/screenshot-sql.png" alt="Lux SQL editor with syntax highlighting and query results" width="800" />
  <br />
  <em>SQL editor with syntax highlighting, autocomplete, and multi-tab support</em>
</p>

<p align="center">
  <img src="docs/screenshots/screenshot-er.png" alt="Lux ER diagram showing entity relationships with foreign key lines" width="800" />
  <br />
  <em>ER diagram with foreign key lines, zoom, and pan</em>
</p>

## Why Lux?

Every PostgreSQL GUI is either bloated, expensive, or drags in a JVM.
Lux compiles to a **3.4 MB binary** that serves a full web UI from memory —
no Electron, no Python, no npm. Build it once, copy it anywhere, run it.

**Your data never leaves your machine.** No cloud, no telemetry, no accounts,
no license checks. Lux binds to `127.0.0.1` and talks directly to your
PostgreSQL server.

```bash
./lux
# Open http://127.0.0.1:8080 and connect from the UI
```

### How Lux compares

| | Lux | pgAdmin | DBeaver | TablePlus | Beekeeper Studio |
|---|---|---|---|---|---|
| **Binary size** | 3.4 MB | ~200 MB | ~115 MB | ~90 MB | ~160 MB |
| **Price** | Free (MIT) | Free | Free / $12/mo | $89 | Free / $7/mo |
| **Runtime deps** | libpq only | Python, JS | JVM | None | Electron |
| **Startup** | Instant | Seconds | Seconds | Fast | Seconds |
| **Privacy** | Fully local | Local | Telemetry | License check | Telemetry |
| **ER diagrams** | Built-in | Plugin | Built-in | No | Paid |
| **Self-contained** | Single binary | Multi-process | Multi-process | App bundle | App bundle |

## Features

- **Table browser** — pagination, sorting, inline cell editing, column resizing,
  multi-row select (Shift/Ctrl+Click), bulk operations
- **SQL editor** — syntax highlighting, autocomplete (tables, columns, keywords,
  functions), Ctrl+Enter execution, EXPLAIN ANALYZE, preview mode, multi-statement
  scripts, multiple tabs, formatting
- **Schema tree** — tables, views, columns with types, primary keys, foreign keys,
  enums, nullability
- **ER diagram** — canvas-based entity relationship view with FK lines, zoom, pan,
  hover highlight
- **Change journal** — every write is logged with one-click undo
- **Row operations** — insert form (respects defaults, NOT NULL, FKs), delete with
  confirmation, truncate table
- **Bulk find & replace** — across column values with preview
- **Foreign key lookup** — autocomplete dropdown when editing FK columns
- **SQL safety** — warns on UPDATE/DELETE without WHERE, preview wraps in
  BEGIN/ROLLBACK, read-only mode blocks all writes (whitelist-based)
- **Export / import** — CSV (RFC 4180) and JSON per table or SQL result, CSV import
  with header validation
- **Command palette** — Ctrl+K quick switcher for tables, columns, actions
- **Connection manager** — save, color-code, and switch between databases
- **Dark + light theme** — Tokyo Night color scheme, respects `prefers-color-scheme`
- **Table creation** — CREATE TABLE modal with column types, PK, NOT NULL
- **Context menu** — right-click cells for copy, edit, NULL, delete, filter, detail
- **DDL viewer** — view and copy CREATE TABLE statements
- **Query history** — browse past queries with timing and row counts
- **Keyboard-driven** — 17 shortcuts, arrow key navigation, Tab/Enter editing
- **Accessible** — ARIA landmarks, focus traps, `prefers-reduced-motion`, WCAG AA contrast
- **Zero dependencies** — no JS frameworks, no CSS libraries, no CDN, no npm

## Getting Started

### Docker (easiest)

Nothing to install except Docker.

```bash
docker build -t lux .
docker run -p 8080:8080 lux
```

Scratch-based image (~7 MB), runs as `nobody`. Connect to your database from
the UI, or pass `--pg` to auto-connect:

```bash
docker run -p 8080:8080 lux --pg "postgresql://user:pass@host.docker.internal/mydb"
```

### Build from source

**Prerequisites:** [Zig 0.13+](https://ziglang.org/download/) and libpq.

| Platform | Install libpq |
|----------|--------------|
| Debian/Ubuntu | `sudo apt install libpq-dev` |
| Fedora/RHEL | `sudo dnf install libpq-devel` |
| Arch | `sudo pacman -S postgresql-libs` |
| macOS (Homebrew) | `brew install libpq` |
| Windows | Install [PostgreSQL](https://www.postgresql.org/download/windows/) (includes libpq) |

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build -Doptimize=ReleaseSafe

./zig-out/bin/lux
```

The binary has no runtime dependencies beyond libpq — copy it anywhere and run it.
Open [http://127.0.0.1:8080](http://127.0.0.1:8080) and enter your connection
string in the UI.

### Development

```bash
zig build test                      # run all tests
zig build -Doptimize=ReleaseSafe    # release build gate
zig build lint                      # zlint + JS quality + format check
```

The lint step requires [zlint](https://github.com/DonIsaac/zlint/releases) on your PATH.
See [CONTRIBUTING.md](CONTRIBUTING.md) for the full development workflow.

### Options

```bash
./lux                          # start and connect from the UI
./lux --pg "postgresql://..."  # auto-connect on startup
./lux -p 3000                  # custom port (default: 8080)
./lux -b 0.0.0.0              # bind to all interfaces (default: 127.0.0.1)
```

Any valid PostgreSQL connection string works — local, remote, or cloud
(RDS, Supabase, Neon, etc.).

## Architecture

```
src/
├── server/
│   ├── main.zig       CLI entry point, arg parsing
│   └── postgres.zig   libpq connector, schema discovery
├── handlers/
│   ├── crud.zig       Insert, update, delete, bulk ops
│   ├── schema.zig     Connect, refresh, connections
│   ├── export.zig     CSV/JSON export, DDL, stats
│   └── sql.zig        SQL execution, history, undo
├── lib/
│   ├── utils.zig      HTTP helpers, escaping, CSRF
│   └── sql_guard.zig  SQL safety analysis (read-only mode)
├── web.zig            HTTP server, routing, state
└── static/
    ├── index.html     UI shell, semantic HTML
    ├── css/styles.css Dark + light themes, responsive
    └── js/            Modular JS (state, grid, crud, sql, ...)
```

Everything is embedded into the binary via `@embedFile` —
no external assets at runtime. The HTTP server is hand-written in Zig with zero
library dependencies.

### Security

- Binds to `127.0.0.1` by default — no network exposure unless you use `--bind`
- CSRF protection via Origin header checks
- Content-Security-Policy headers on all HTML responses
- SQL identifier escaping prevents injection
- Read-only mode uses a whitelist (not a blacklist)
- Saved connections stored with `0600` permissions

See [SECURITY.md](SECURITY.md) for the full security policy.

### Limitations

Lux is a **local development tool**. It is not designed for production database
administration. If you need production-grade tooling, use pgAdmin, DBeaver, or
dedicated DB management software.

**Single-threaded server.** Lux handles one request at a time. A slow query
blocks all other requests (CSS, schema refreshes, etc.) until it completes.
This is fine when you're the only user on localhost — not fine for shared or
production use.

**In-memory change journal.** The undo history lives in server memory only. If
the process crashes or you restart Lux, the journal is gone. It's a session
safety net, not an audit log.

**Read-only mode is a convenience guard.** It catches accidental writes
(DELETE without WHERE, etc.) via string-level SQL analysis. It is not a security
boundary — use PostgreSQL roles (`GRANT SELECT`) for real enforcement.

**Tables without primary keys use `ctid`.** For tables missing a primary key,
Lux identifies rows by PostgreSQL's internal `ctid` (physical row address).
If `VACUUM` reorganizes the table between loading and editing, the `ctid` may
point to a different row. This is extremely unlikely in practice, but the fix
is simple: add a primary key to your tables.

## Configuration

Connection strings are never stored on disk unless you explicitly save them via
the connection manager. Saved connections go to:

| Platform | Path |
|----------|------|
| Linux | `$XDG_CONFIG_HOME/lux/connections.json` or `~/.config/lux/connections.json` |
| macOS | `~/Library/Application Support/lux/connections.json` |
| Windows | `%APPDATA%\lux\connections.json` |

The change journal is in-memory — it resets when the server restarts.
It's a safety net for the current session, not a persistent audit log.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and code style.

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build test                      
zig build -Doptimize=ReleaseSafe    
```

Both build gates must pass before any change is considered done.

## License

MIT — see [LICENSE](LICENSE).
