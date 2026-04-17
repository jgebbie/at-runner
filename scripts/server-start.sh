#!/usr/bin/env bash
# Start the at-runner gRPC server (default: Docker).
# Usage:
#   ./scripts/server-start.sh            # Docker (default)
#   ./scripts/server-start.sh --docker   # Docker (explicit)
#   ./scripts/server-start.sh --local    # local Rust binary (needs ./bin)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$REPO/.server.pid"
PORT="${AT_RUNNER_PORT:-50051}"

MODE="${1:-}"
if [[ -z "$MODE" || "$MODE" == "--docker" ]]; then
    echo "==> Starting at-runner via Docker on port $PORT"
    AT_IMAGE="${AT_IMAGE:-ghcr.io/jgebbie/at:at_2026_2_2}"

    # Build if image doesn't exist
    if ! docker image inspect at-runner >/dev/null 2>&1; then
        echo "    Building Docker image (first time takes ~1 min)..."
        docker build --build-arg "AT_IMAGE=$AT_IMAGE" -t at-runner "$REPO"
    fi

    # Stop any existing container
    docker rm -f at-runner-dev 2>/dev/null || true

    # Use an empty writable workspace. Tests upload the fixtures they need,
    # and production runs should not inherit host-side fixture directories.
    docker run -d --name at-runner-dev \
        -p "$PORT:50051" \
        --tmpfs /workspace:rw,noexec,nosuid,size=512m \
        at-runner

    echo "docker" > "$PIDFILE"
    echo "==> Container at-runner-dev running on port $PORT"
    echo "    Stop with: ./scripts/server-stop.sh"
    exit 0
fi

if [[ "$MODE" != "--local" ]]; then
    echo "Unknown option: ${MODE}"
    echo "Usage: ./scripts/server-start.sh [--docker|--local]"
    exit 2
fi

# --- Local mode ---

SERVER="$REPO/target/release/at-runner"
WORKSPACE="/tmp/at-workspace-$$"

if [[ -f "$PIDFILE" ]]; then
    OLD_PID=$(cat "$PIDFILE")
    if [[ "$OLD_PID" == "docker" ]]; then
        echo "Docker server is running. Stop it first: ./scripts/server-stop.sh"
        exit 1
    elif kill -0 "$OLD_PID" 2>/dev/null; then
        echo "Server already running (PID $OLD_PID). Stop it first: ./scripts/server-stop.sh"
        exit 1
    fi
fi

if [[ ! -x "$SERVER" ]]; then
    echo "==> Building server..."
    (cd "$REPO" && PATH="$HOME/.local/bin:$PATH" cargo build --release -p at-runner)
fi

# libgfortran for the AT executables
if [[ -d "$HOME/.local/lib" ]]; then
    export LD_LIBRARY_PATH="${HOME}/.local/lib:${LD_LIBRARY_PATH:-}"
fi

mkdir -p "$WORKSPACE"

echo "==> Starting at-runner on port $PORT (workspace: $WORKSPACE)"
"$SERVER" \
    --bin-dir "$REPO/bin" \
    --workspace "$WORKSPACE" \
    --port "$PORT" \
    > "$REPO/.server.log" 2>&1 &

PID=$!
echo "$PID" > "$PIDFILE"

# Wait for it to be ready
for i in $(seq 1 20); do
    if grep -q "starting at-runner" "$REPO/.server.log" 2>/dev/null; then
        echo "==> Server running (PID $PID, log: .server.log)"
        echo "    Stop with: ./scripts/server-stop.sh"
        exit 0
    fi
    sleep 0.2
done

echo "==> Server may have failed to start. Check .server.log"
cat "$REPO/.server.log"
exit 1
