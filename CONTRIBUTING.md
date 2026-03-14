# Contributing to Lux

Thanks for your interest in contributing. This document covers the workflow and rules.

## Development Setup

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build test      # verify everything works
```

### Linter setup

`zig build lint` runs all linters in one command — Zig and frontend:

```
zig build lint
  ├── zlint        (Zig linting)
  ├── zig fmt      (Zig formatting)
  └── biome check  (JS/CSS linting + formatting)
```

Both external tools must be installed or the lint gate will fail:

```bash
# 1. zlint — Zig linter
#    https://github.com/DonIsaac/zlint/releases
#    Download the prebuilt binary and place it on your PATH.

# 2. Biome — JS/CSS linter and formatter
#    https://github.com/biomejs/biome/releases
#    Installed as an npm dev dependency — just run:
npm install
```

## Build Gates

Every change must pass all three gates before it's considered done:

```bash
zig build test                      # all tests pass
zig build -Doptimize=ReleaseSafe    # release build succeeds
zig build lint                      # zlint + zig fmt + biome (see above)
```

## Code Style

| Element | Convention | Example |
|---------|-----------|---------|
| Files | snake_case | `postgres.zig` |
| Functions | camelCase | `fetchSchema()` |
| Types | PascalCase | `ServerState` |
| Constants | snake_case | `max_request_size` |

## Rules

- No `@panic` in production code — use error returns
- No discarded errors — always `try`, `catch`, or handle explicitly
- `defer` cleanup immediately after every resource acquisition

See the project rules in the repository for the full engineering constitution.

## Pull Requests

- Keep PRs focused on a single change
- Include test coverage for new functionality
- All three gates must pass (`zig build test` + `zig build -Doptimize=ReleaseSafe` + `zig build lint`)
- Reference related issues if applicable
