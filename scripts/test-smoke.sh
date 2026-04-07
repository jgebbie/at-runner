#!/usr/bin/env bash
# Quick smoke test: exercises all three API tiers against a running server.
# Usage: ./scripts/test-smoke.sh [target]
#   target defaults to localhost:50051
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-${AT_RUNNER_TARGET:-localhost:50051}}"
VENV="$REPO/client/python/.venv"

# Fortran fixtures: https://github.com/jgebbie/at (tests/ tree)
# shellcheck source=ensure-at-tests.sh
source "$REPO/scripts/ensure-at-tests.sh"

source "$VENV/bin/activate"

echo "==> Smoke test against $TARGET"
echo

PYTHONPATH="$REPO/client/python/src" python3 - "$TARGET" "$REPO" <<'PYEOF'
import os, sys, time
from pathlib import Path

target = sys.argv[1]
repo   = Path(sys.argv[2])

sys.path.insert(0, str(repo / "client/python/src"))
from at_runner import run_sync, ATSession, Step

test_dir = Path(os.environ["AT_TESTS_ROOT"]) / "Munk"
env_data = (test_dir / "MunkK.env").read_bytes()
flp_data = (test_dir / "MunkK.flp").read_bytes()

passed = 0
failed = 0

def check(label, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  \033[32m✓\033[0m {label}")
    else:
        failed += 1
        print(f"  \033[31m✗\033[0m {label}  {detail}")

# ── Tier 1: RunSync ─────────────────────────────────────────
print("── Tier 1: RunSync ──")
t0 = time.time()
r = run_sync(target, model="kraken", file_root="MunkK",
             inputs={"MunkK.env": env_data})
dt = time.time() - t0
check(f"kraken MunkK  exit={r.exit_code}  {dt:.3f}s  files={sorted(r.files.keys())}",
      r.exit_code == 0 and "MunkK.mod" in r.files and "MunkK.prt" in r.files)

r = run_sync(target, model="kraken", file_root="MunkS",
             inputs={"MunkS.env": (test_dir / "MunkS.env").read_bytes()})
check(f"kraken MunkS  exit={r.exit_code}  files={sorted(r.files.keys())}",
      r.exit_code == 0 and "MunkS.mod" in r.files)

try:
    run_sync(target, model="nonexistent", file_root="x", inputs={})
    check("unknown model rejected", False, "no error raised")
except Exception as e:
    check("unknown model rejected", "INVALID_ARGUMENT" in str(e))

# ── Tier 2: Run + Workspace ─────────────────────────────────
print("\n── Tier 2: Run + Workspace ──")
s = ATSession(target)

s.upload("MunkK.env", env_data)
r = s.run("kraken", "MunkK")
check(f"kraken → workspace  exit={r.exit_code}  files={sorted(r.files.keys())}",
      r.exit_code == 0 and "MunkK.mod" in r.files)

s.upload("MunkK.flp", flp_data)
r = s.run("field", "MunkK")
check(f"field reads .mod    exit={r.exit_code}  files={sorted(r.files.keys())}",
      r.exit_code == 0 and "MunkK.shd" in r.files)

s.upload("tmp.txt", b"hello")
data = s.download("tmp.txt")
check("upload → download roundtrip", data == b"hello")
s.delete("tmp.txt")
names = [f[0] for f in s.list_files()]
check("delete removes file", "tmp.txt" not in names)

chunks = []
s.upload("MunkK.env", env_data)  # re-upload for clean run
r = s.run("kraken", "MunkK", on_output=lambda st, d: chunks.append(1))
check(f"streaming callback fires ({len(chunks)} chunks)", r.exit_code == 0)
s.close()

# ── Tier 3: RunPipeline ─────────────────────────────────────
print("\n── Tier 3: RunPipeline ──")
s = ATSession(target)

t0 = time.time()
r = s.run_pipeline([
    Step("k1", "kraken", "MunkK", inputs={"MunkK.env": env_data}),
    Step("f1", "field",  "MunkK", inputs={"MunkK.flp": flp_data}, depends_on=["k1"]),
])
dt = time.time() - t0
k1_files = sorted(r.steps["k1"].files.keys()) if "k1" in r.steps else []
f1_files = sorted(r.steps["f1"].files.keys()) if "f1" in r.steps else []
check(f"kraken→field pipeline  {dt:.3f}s  k1={k1_files}  f1={f1_files}",
      r.all_succeeded
      and "MunkK.mod" in r.steps.get("k1", Step("","","")).files
      and "MunkK.shd" in r.steps.get("f1", Step("","","")).files)

r = s.run_pipeline([
    Step("bad", "kraken", "nonexistent", inputs={"nonexistent.env": b"junk"}),
    Step("dep", "field",  "nonexistent", inputs={"nonexistent.flp": b"junk"}, depends_on=["bad"]),
])
check(f"failed dep → skip  skipped={r.skipped_steps}",
      not r.all_succeeded)

s.close()

# ── Summary ──────────────────────────────────────────────────
print(f"\n{'─'*50}")
total = passed + failed
if failed == 0:
    print(f"\033[32m  {passed}/{total} passed\033[0m")
else:
    print(f"\033[31m  {passed}/{total} passed, {failed} failed\033[0m")
sys.exit(1 if failed else 0)
PYEOF
