#!/usr/bin/env bash
# Run the sweep test against a docker-compose runner pool.
#
# Usage:
#   ./scripts/test-sweep-compose.sh
#   AT_TESTS_COMPOSE_MOUNT=/abs/path/to/at/tests ./scripts/test-sweep-compose.sh
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"

BUILD_AT_LOCAL="${BUILD_AT_LOCAL:-1}"
AT_IMAGE="${AT_IMAGE:-}"

if [[ "$BUILD_AT_LOCAL" == "1" ]]; then
  if [[ ! -d "$REPO/external/at" ]]; then
    echo "error: external/at not found. Run ./scripts/fetch-at-tests.sh first, or set BUILD_AT_LOCAL=0." >&2
    exit 1
  fi

  echo "==> Building local AT binaries image (portable flags) from external/at"
  docker buildx build \
    --load \
    -t at-binaries-local \
    -f "$REPO/external/at/.github/docker/Dockerfile.artifacts" \
    "$REPO/external/at"
  AT_IMAGE="at-binaries-local"
fi

echo "==> Building at-runner image (tag: at-runner)"
if [[ -n "$AT_IMAGE" ]]; then
  docker build --build-arg AT_IMAGE="$AT_IMAGE" -t at-runner "$REPO"
else
  docker build -t at-runner "$REPO"
fi

echo "==> Shutting down any existing compose pool"
cd "$REPO/testing"
docker compose down >/dev/null 2>&1 || true

echo "==> Starting runner pool + running sweep driver"
docker compose up --build --abort-on-container-exit sweep-driver
