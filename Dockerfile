# ── Build stage ──────────────────────────────────────────────────────
FROM alpine:3.21 AS build

RUN apk add --no-cache curl xz postgresql-dev musl-dev

# Install Zig 0.13.0 (pinned for reproducible builds)
RUN curl -L https://ziglang.org/download/0.13.0/zig-linux-x86_64-0.13.0.tar.xz \
    | tar -xJ -C /opt \
    && ln -s /opt/zig-linux-x86_64-0.13.0/zig /usr/local/bin/zig

WORKDIR /app

# Copy build files first for layer caching — source changes don't re-download Zig
COPY build.zig build.zig.zon ./
COPY src/ src/

# ReleaseSmall: ~430KB binary (vs ~3.4MB ReleaseSafe)
RUN zig build -Doptimize=ReleaseSmall

# Resolve libpq symlink to actual file for COPY
RUN cp --dereference /usr/lib/libpq.so.5 /tmp/libpq.so.5

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

# Runtime shared libraries (musl libc + libpq + TLS)
COPY --from=build /lib/ld-musl-x86_64.so.1 /lib/ld-musl-x86_64.so.1
COPY --from=build /tmp/libpq.so.5 /usr/lib/libpq.so.5
COPY --from=build /usr/lib/libssl.so.3 /usr/lib/libssl.so.3
COPY --from=build /usr/lib/libcrypto.so.3 /usr/lib/libcrypto.so.3

USER nobody
EXPOSE 8080

# Bind to 0.0.0.0 inside container so Docker port mapping works.
# Default: 127.0.0.1 outside Docker (safe — no network exposure).
ENTRYPOINT ["/lux", "--bind", "0.0.0.0"]
