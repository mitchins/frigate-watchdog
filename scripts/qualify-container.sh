#!/usr/bin/env sh
# Build the production image and run the container qualification suite.
set -eu
cd "$(dirname "$0")/.."
docker build -t frigate-watchdog:local .
rm -rf tests/container/watchdog-data
mkdir -p tests/container/watchdog-data
# The watchdog runs as UID 1000. Make the bind mount writable for it where
# the platform allows (Linux CI: sudo chown). Never widen permissions: if the
# directory stays unwritable, the watchdog itself fails loudly with a
# permission error instead of silently passing.
chown -R 1000:1000 tests/container/watchdog-data 2>/dev/null \
    || sudo -n chown -R 1000:1000 tests/container/watchdog-data 2>/dev/null \
    || true
set +e
docker compose -f tests/container/compose.test.yaml up \
    --abort-on-container-exit --exit-code-from qualify --build
status=$?
set -e
docker compose -f tests/container/compose.test.yaml down --remove-orphans 2>/dev/null || true
exit $status
