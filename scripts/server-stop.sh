#!/usr/bin/env bash
# Stop a running at-runner server (local or Docker).
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
PIDFILE="$REPO/.server.pid"

if [[ ! -f "$PIDFILE" ]]; then
    echo "No server PID file found. Nothing to stop."
    exit 0
fi

CONTENT=$(cat "$PIDFILE")

if [[ "$CONTENT" == "docker" ]]; then
    echo "==> Stopping Docker container at-runner-dev"
    docker rm -f at-runner-dev 2>/dev/null || true
    rm -f "$PIDFILE"
    echo "    Stopped."
    exit 0
fi

PID="$CONTENT"
if kill -0 "$PID" 2>/dev/null; then
    echo "==> Stopping server (PID $PID)"
    kill "$PID"
    # Wait up to 5 seconds for graceful shutdown
    for i in $(seq 1 25); do
        kill -0 "$PID" 2>/dev/null || break
        sleep 0.2
    done
    if kill -0 "$PID" 2>/dev/null; then
        echo "    Force-killing..."
        kill -9 "$PID" 2>/dev/null || true
    fi
    echo "    Stopped."
else
    echo "    Server (PID $PID) was not running."
fi

rm -f "$PIDFILE"
