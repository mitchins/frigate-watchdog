# frigate-watchdog container: small, non-root, read-only-rootfs friendly.
#
# Build context must contain: pyproject.toml, uv.lock, src/, README.md, LICENSE

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
WORKDIR /app

# Dependencies first for layer caching; locked, no dev group, no cache.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project --no-editable

# Then the project itself.
COPY src ./src
COPY README.md LICENSE ./
RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim-bookworm AS runtime

# Run as an unprivileged user with a home for occasional tooling state.
RUN groupadd --gid 1000 watchdog \
    && useradd --uid 1000 --gid 1000 --system --create-home watchdog \
    && mkdir -p /data /config \
    && chown -R watchdog:watchdog /data /config

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WATCHDOG_CONFIG=/config/config.yaml \
    WATCHDOG_DATA_DIR=/data

COPY --from=builder --chown=watchdog:watchdog /app/.venv /app/.venv

USER 1000:1000
WORKDIR /app
VOLUME ["/data"]

# The status API binds loopback inside the container; publishing is opt-in.
EXPOSE 8080

# Uses only the standard library: valid under a read-only root filesystem.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
    CMD ["python", "-c", "import sys,urllib.request;sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=4).status==200 else 1)"]

ENTRYPOINT ["fwatch"]
CMD ["serve"]
