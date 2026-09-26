#!/usr/bin/env sh
# Build the production image and run the container qualification suite.
set -eu
cd "$(dirname "$0")/.."
docker build -t frigate-watchdog:local .
rm -rf tests/container/watchdog-data
mkdir -p tests/container/watchdog-data
# The watchdog runs as UID 1000; a root-owned bind mount is unwritable.
chown -R 1000:1000 tests/container/watchdog-data 2>/dev/null || chmod 777 tests/container/watchdog-data
set +e
docker compose -f tests/container/compose.test.yaml up \
    --abort-on-container-exit --exit-code-from qualify --build
status=$?
set -e
docker compose -f tests/container/compose.test.yaml down --remove-orphans 2>/dev/null || true
exit $status
