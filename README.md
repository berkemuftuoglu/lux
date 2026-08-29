<p align="center">
  <img src="docs/logo.svg" alt="Lux logo" width="128" height="128" />
</p>

<h1 align="center">Lux</h1>

<p align="center">
  A lightweight, local-first PostgreSQL web client.
  <br />
  Single binary. Zero install. Modern UI compiled into one ~1.4 MB Zig executable. No Electron, no JVM, no Python.
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="MIT License" /></a>
  <a href="https://ziglang.org"><img src="https://img.shields.io/badge/built%20with-Zig%200.15.2-f7a41d.svg" alt="Built with Zig" /></a>
</p>

<p align="center">
  <img src="docs/screenshots/01-grid.png" alt="Lux table browser: schema tree, data grid, 1,247 rows" width="900" />
</p>

## Why Lux?

Every PostgreSQL GUI is either bloated, expensive, or drags in a JVM.
Lux compiles to a **~1.4 MB binary** (ReleaseSmall) that serves a full web UI from memory.
No Electron, no Python, and no PostgreSQL client library: the wire protocol is spoken in
pure Zig, so `ldd` shows nothing but libc. Download it and run it.

| | Lux | pgAdmin | DBeaver | TablePlus | Beekeeper Studio |
|---|---|---|---|---|---|
| **Binary size** | ~1.4 MB | ~200 MB | ~115 MB | ~90 MB | ~160 MB |
| **Price** | Free (MIT) | Free | Free / $12/mo | $89 | Free / $7/mo |
| **Runtime deps** | None (libc) | Python, JS | JVM | None | Electron |
| **Privacy** | Fully local | Local | Telemetry | License check | Telemetry |

Table browser, SQL editor with autocomplete, CSV/JSON export, CSV import,
pending-edit journal, command palette, dark/light themes, read-only mode.
<details>
<summary><b>More screenshots</b></summary>

<br />

**SQL editor** — CodeMirror 6 with autocomplete, results inline.

<img src="docs/screenshots/03-sql.png" alt="SQL editor with a grouped aggregate query and its results" width="900" />

**JSON/JSONB columns** rendered as stored.

<img src="docs/screenshots/02-jsonb.png" alt="audit_log table showing a jsonb payload column" width="900" />

**Command palette** — Ctrl+K.

<img src="docs/screenshots/04-palette.png" alt="Command palette filtering tables" width="900" />

</details>

## Install

### Download (recommended)

Grab the binary for your platform from the [latest release](https://github.com/berkemuftuoglu/lux/releases/latest):

| OS | Architecture | Artifact |
|---|---|---|
| Linux | x86_64 | `lux-v0.1.0-linux-x86_64` |
| Linux | ARM64 | `lux-v0.1.0-linux-aarch64` |
| macOS | Intel | `lux-v0.1.0-macos-x86_64` |
| macOS | Apple Silicon | `lux-v0.1.0-macos-aarch64` |
| Windows | x86_64 | `lux-v0.1.0-windows-x86_64.exe` |

**Linux / macOS:**

```bash
chmod +x lux-v0.1.0-* && ./lux-v0.1.0-*
```

**Windows (PowerShell):**

```powershell
.\lux-v0.1.0-windows-x86_64.exe
```

Then open <http://127.0.0.1:8080> and connect from the UI.

There is nothing else to install. Lux talks to PostgreSQL over the wire protocol in
pure Zig, so there is no libpq to install and no client library to keep in sync.

#### First-run friction (unsigned binaries)

v0.1.0 binaries are not code-signed yet (Apple Developer ID and Windows EV certificates are tracked for a later release). One-time workaround per OS:

- **macOS** — Gatekeeper will refuse to open the binary. Run once:
  ```bash
  xattr -d com.apple.quarantine ./lux-v0.1.0-macos-*
  ```
  After that, double-click or `./lux-...` works normally.

- **Windows** — SmartScreen shows "Windows protected your PC". Click **More info** → **Run anyway**.

- **Linux** — No friction. Binary runs after `chmod +x`.

### Build from source

Requires [Zig 0.15.2+](https://ziglang.org/download/). No system libraries, no headers.

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build -Doptimize=ReleaseSmall
./zig-out/bin/lux
```

Use `-Doptimize=ReleaseSafe` instead for a development build with safety checks (larger: ~6 MB vs ~1.4 MB).

### Docker

```bash
docker build -t lux .
docker run -p 8080:8080 lux
```

### Options

```
./lux                          # start and connect from the UI
./lux --pg "postgresql://..."  # auto-connect on startup
./lux -p 3000                  # custom port (default: 8080)
./lux -b 0.0.0.0               # bind to all interfaces (default: 127.0.0.1)
```

Works with any PostgreSQL — local, remote, or cloud (RDS, Supabase, Neon).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). All three build gates must pass:

```bash
zig build test                      # 127 Zig unit tests
zig build -Doptimize=ReleaseSafe    # release build succeeds
zig build lint                      # zlint + zig fmt + biome
```

Frontend type-check and unit/component tests:

```bash
cd frontend && pnpm run check && pnpm test   # svelte-check + 15 vitest tests
```

End-to-end smoke (Playwright, 24 user flows) against a live Postgres:

```bash
cd e2e && npm test
```

## Security

See [SECURITY.md](SECURITY.md) for the threat model and disclosure policy. Short version: Lux is local-first, binds to 127.0.0.1 by default, and trusts the local user.

## License

MIT — see [LICENSE](LICENSE).
