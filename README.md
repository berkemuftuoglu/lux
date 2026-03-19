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
</p>

## Why Lux?

Every PostgreSQL GUI is either bloated, expensive, or drags in a JVM.
Lux compiles to a **~4 MB binary** that serves a full web UI from memory.
No Electron, no Python, no npm.

| | Lux | pgAdmin | DBeaver | TablePlus | Beekeeper Studio |
|---|---|---|---|---|---|
| **Binary size** | ~4 MB | ~200 MB | ~115 MB | ~90 MB | ~160 MB |
| **Price** | Free (MIT) | Free | Free / $12/mo | $89 | Free / $7/mo |
| **Runtime deps** | libpq only | Python, JS | JVM | None | Electron |
| **Privacy** | Fully local | Local | Telemetry | License check | Telemetry |

Table browser, SQL editor with autocomplete, ER diagrams, CSV/JSON export,
change journal with undo, dark/light themes, read-only mode.
[More screenshots](docs/screenshots/).

## Getting Started

### Docker

```bash
docker build -t lux .
docker run -p 8080:8080 lux
```

### Build from source

Requires [Zig 0.13+](https://ziglang.org/download/) and libpq (`apt install libpq-dev` / `brew install libpq`).

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build -Doptimize=ReleaseSafe
./zig-out/bin/lux
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) and connect from the UI.

### Options

```
./lux                          # start and connect from the UI
./lux --pg "postgresql://..."  # auto-connect on startup
./lux -p 3000                  # custom port (default: 8080)
./lux -b 0.0.0.0              # bind to all interfaces (default: 127.0.0.1)
```

Works with any PostgreSQL — local, remote, or cloud (RDS, Supabase, Neon).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Both build gates must pass:

```bash
zig build test
zig build -Doptimize=ReleaseSafe
```

## License

MIT — see [LICENSE](LICENSE).
