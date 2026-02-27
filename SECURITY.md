# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in Lux, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities.
2. Use [GitHub's private vulnerability reporting](https://github.com/berkemuftuoglu/lux/security/advisories/new) on this repository.
3. Include steps to reproduce the issue if possible.

## Scope

Lux is a local-first tool designed to run on trusted networks. The following are known design decisions, not vulnerabilities:

- **No authentication**: Lux binds to `127.0.0.1` by default and trusts the local user.
- **Saved connections**: Connection strings (including passwords) are stored in plaintext in `connections.json` with owner-only permissions (0600).

## Supported Versions

Only the latest release is supported with security updates.
