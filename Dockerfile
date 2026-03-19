# ── Build stage ──────────────────────────────────────────────────────
FROM alpine:3.21 AS build

RUN apk add --no-cache curl xz musl-dev

# Install Zig 0.15.2 (pinned for reproducible builds)
RUN curl -L https://ziglang.org/download/0.15.2/zig-linux-x86_64-0.15.2.tar.xz \
    | tar -xJ -C /opt \
    && ln -s /opt/zig-linux-x86_64-0.15.2/zig /usr/local/bin/zig

WORKDIR /app

# Copy build files first for layer caching — source changes don't re-download Zig
COPY build.zig build.zig.zon ./
COPY src/ src/

# ReleaseSmall: ~430KB binary (vs ~3.4MB ReleaseSafe)
RUN zig build -Doptimize=ReleaseSmall

# ── Runtime stage ────────────────────────────────────────────────────
# scratch = no shell, no package manager, no attack surface
FROM scratch

# OCI image labels
LABEL org.opencontainers.image.title="Lux" \
      org.opencontainers.image.description="Lightweight PostgreSQL web client" \
      org.opencontainers.image.url="https://github.com/berkemuftuoglu/lux" \
      org.opencontainers.image.source="https://github.com/berkemuftuoglu/lux" \
      org.opencontainers.image.licenses="MIT"

# passwd for USER nobody
COPY --from=build /etc/passwd /etc/passwd

# Binary
COPY --from=build /app/zig-out/bin/lux /lux

# Runtime shared libraries (musl libc only — pg.zig is pure Zig, no libpq needed)
COPY --from=build /lib/ld-musl-x86_64.so.1 /lib/ld-musl-x86_64.so.1

USER nobody
EXPOSE 8080

# Bind to 0.0.0.0 inside container so Docker port mapping works.
# Default: 127.0.0.1 outside Docker (safe — no network exposure).
ENTRYPOINT ["/lux", "--bind", "0.0.0.0"]
