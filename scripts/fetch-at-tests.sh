#!/usr/bin/env bash
# Shallow-clone the Acoustics Toolbox repo (Fortran sources + tests/).
# Default: https://github.com/jgebbie/at — same tree as the canonical tests/ layout.
#
# Environment (optional):
#   AT_AT_REPO_URL   — git URL (default: https://github.com/jgebbie/at.git)
#   AT_AT_REPO_DIR   — clone destination (default: <repo>/external/at)
#   AT_AT_REPO_REF   — branch or tag (default: main)
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
URL="${AT_AT_REPO_URL:-https://github.com/jgebbie/at.git}"
DEST="${AT_AT_REPO_DIR:-$REPO/external/at}"
REF="${AT_AT_REPO_REF:-main}"

if [[ -d "$DEST/.git" ]]; then
  echo "AT repo already present at $DEST (delete it to re-clone, or run git -C \"$DEST\" pull)"
  exit 0
fi

if [[ -e "$DEST" ]]; then
  echo "error: $DEST exists but is not a git clone; remove it or set AT_AT_REPO_DIR" >&2
  exit 1
fi

echo "==> Cloning $URL (branch $REF) -> $DEST"
mkdir -p "$(dirname "$DEST")"
git clone --depth 1 --branch "$REF" "$URL" "$DEST"
