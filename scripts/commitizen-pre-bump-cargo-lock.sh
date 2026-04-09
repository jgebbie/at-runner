#!/usr/bin/env bash
# Run from Commitizen pre_bump_hooks after version fields are written to Cargo.toml files.
# Refreshes the workspace Cargo.lock so the following bump commit includes it.
set -euo pipefail
ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"
exec cargo build --workspace
