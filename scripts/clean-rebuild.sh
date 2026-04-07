#!/usr/bin/env bash
# Clean local artifacts + Docker image/container, then rebuild and smoke test.
# Usage:
#   ./scripts/clean-rebuild.sh
#   CLEAN_VENV=0 ./scripts/clean-rebuild.sh        # keep client/python/.venv
#   CLEAN_AT_TESTS=0 ./scripts/clean-rebuild.sh    # skip fetch-at-tests.sh
#   PULL_AT=1 ./scripts/clean-rebuild.sh           # docker pull ghcr.io/jgebbie/at:latest first
#   BUILD_AT_LOCAL=1 ./scripts/clean-rebuild.sh    # build a local AT binaries image from external/at
#   AT_IMAGE=at-binaries-local ./scripts/clean-rebuild.sh  # use a specific AT image for at-runner
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"

CLEAN_VENV="${CLEAN_VENV:-1}"
CLEAN_AT_TESTS="${CLEAN_AT_TESTS:-1}"
PULL_AT="${PULL_AT:-0}"
BUILD_AT_LOCAL="${BUILD_AT_LOCAL:-1}"
AT_IMAGE="${AT_IMAGE:-}"

echo "==> Cleaning at-runner workspace"
rm -f "$REPO/.server.pid" "$REPO/.server.log" 2>/dev/null || true

if [[ "$CLEAN_VENV" == "1" ]]; then
  echo "==> Removing Python venv"
  rm -rf "$REPO/client/python/.venv" 2>/dev/null || true
fi

echo "==> Stopping any running server/container"
"$REPO/scripts/server-stop.sh" || true
docker rm -f at-runner-dev 2>/dev/null || true

echo "==> Removing local Docker image + build cache (repo-scoped)"
docker rmi -f at-runner 2>/dev/null || true
docker buildx prune -f --filter "until=24h" >/dev/null 2>&1 || true

if [[ "$PULL_AT" == "1" ]]; then
  echo "==> Pulling latest AT base image"
  docker pull ghcr.io/jgebbie/at:latest
fi

if [[ "$BUILD_AT_LOCAL" == "1" ]]; then
  echo "==> Building local AT binaries image (portable flags) from external/at"
  if [[ ! -d "$REPO/external/at" ]]; then
    echo "external/at not found. Run ./scripts/fetch-at-tests.sh first."
    exit 1
  fi
  docker buildx build \
    --load \
    -t at-binaries-local \
    -f "$REPO/external/at/.github/docker/Dockerfile.artifacts" \
    "$REPO/external/at"
  AT_IMAGE="at-binaries-local"
fi

if [[ "$CLEAN_AT_TESTS" == "1" ]]; then
  echo "==> Ensuring AT test fixtures are present"
  "$REPO/scripts/fetch-at-tests.sh"
fi

echo "==> Recreating Python env (client/python)"
python3 -m venv "$REPO/client/python/.venv"
source "$REPO/client/python/.venv/bin/activate"
python -m pip install -U pip wheel
python -m pip install -e "$REPO/client/python[dev]"

echo "==> Building and starting server via Docker"
AT_IMAGE="$AT_IMAGE" "$REPO/scripts/server-start.sh" --docker

echo "==> Running smoke test"
"$REPO/scripts/test-smoke.sh"
