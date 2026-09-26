#!/usr/bin/env sh
# Build the production image and run the container qualification suite.
set -eu
cd "$(dirname "$0")/.."
docker build -t frigate-watchdog:local .
rm -rf tests/container/watchdog-data
mkdir -p tests/container/watchdog-data
docker compose -f tests/container/compose.test.yaml up \
    --abort-on-container-exit --exit-code-from qualify --build
status=$?
docker compose -f tests/container/compose.test.yaml down --remove-orphans 2>/dev/null || true
exit $status
