#!/usr/bin/env bash
# Resolve Fortran integration test fixtures and set AT_TESTS_ROOT.
# Source this from other scripts:  source "$(dirname "$0")/ensure-at-tests.sh"
#
# Resolution order:
#   1. AT_TESTS_ROOT if set and points at an existing directory
#   2. <at-runner>/tests if it contains Munk/ (legacy or symlink)
#   3. <at-runner>/external/at/tests after running fetch-at-tests.sh
#
# Optional: AT_AT_REPO_URL, AT_AT_REPO_DIR, AT_AT_REPO_REF (see fetch-at-tests.sh)
_ensure_at_tests__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
_ensure_at_tests__repo="$(cd "$_ensure_at_tests__dir/.." && pwd)"

if [[ -n "${AT_TESTS_ROOT:-}" && -d "$AT_TESTS_ROOT" ]]; then
  export AT_TESTS_ROOT
elif [[ -d "$_ensure_at_tests__repo/tests/Munk" ]]; then
  export AT_TESTS_ROOT="$_ensure_at_tests__repo/tests"
else
  if [[ ! -d "$_ensure_at_tests__repo/external/at/tests/Munk" ]]; then
    "$_ensure_at_tests__dir/fetch-at-tests.sh"
  fi
  export AT_TESTS_ROOT="$_ensure_at_tests__repo/external/at/tests"
fi

if [[ ! -d "$AT_TESTS_ROOT" ]]; then
  echo "error: AT_TESTS_ROOT=$AT_TESTS_ROOT is not a directory" >&2
  exit 1
fi
