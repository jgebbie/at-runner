#!/usr/bin/env bash
# Run the pytest integration suite against a running server.
# Usage: ./scripts/test-integration.sh [target] [extra pytest args...]
#   target defaults to localhost:50051
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${AT_RUNNER_TARGET:-localhost:50051}"
VENV="$REPO/client/python/.venv"

# shellcheck source=ensure-at-tests.sh
source "$REPO/scripts/ensure-at-tests.sh"

source "$VENV/bin/activate"

# Shift off target if first arg looks like a host:port
if [[ "${1:-}" =~ : ]]; then
    TARGET="$1"
    shift
fi

echo "==> Running pytest against $TARGET"
echo

AT_RUNNER_TARGET="$TARGET" \
AT_TESTS_ROOT="$AT_TESTS_ROOT" \
PYTHONPATH="$REPO/client/python/src" \
    pytest "$REPO/client/python/tests/test_integration.py" \
    -v --tb=short "$@"
