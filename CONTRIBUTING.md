# Contributing to Lux

Thanks for your interest in contributing. This document covers the workflow and rules.

## Development Setup

```bash
git clone https://github.com/berkemuftuoglu/lux.git
cd lux
zig build test      # verify everything works
```

## Build Gates

Every change must pass all three gates before it's considered done:

```bash
zig build test                      # all tests pass
zig build -Doptimize=ReleaseSafe    # release build succeeds
./check.sh                          # quality checks pass
```

## Code Style

| Element | Convention | Example |
|---------|-----------|---------|
| Files | snake_case | `postgres.zig` |
| Functions | camelCase | `fetchSchema()` |
| Types | PascalCase | `ServerState` |
| Constants | SCREAMING_SNAKE | `MAX_REQUEST_SIZE` |

## Rules

- No `@panic` in production code — use error returns
- No discarded errors — always `try`, `catch`, or handle explicitly
- `defer` cleanup immediately after every resource acquisition

See the project rules in the repository for the full engineering constitution.

## Pull Requests

- Keep PRs focused on a single change
- Include test coverage for new functionality
- All three gates must pass (`zig build test` + `zig build -Doptimize=ReleaseSafe` + `./check.sh`)
- Reference related issues if applicable
