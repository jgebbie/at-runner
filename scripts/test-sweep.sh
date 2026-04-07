#!/usr/bin/env bash
# Sweep RunSync across every test case in tests/.
# Parses runtests.m to find the correct model and file root for each case.
# Usage: ./scripts/test-sweep.sh [target]
set -euo pipefail

REPO="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-${AT_RUNNER_TARGET:-localhost:50051}}"
VENV="$REPO/client/python/.venv"

# shellcheck source=ensure-at-tests.sh
source "$REPO/scripts/ensure-at-tests.sh"

source "$VENV/bin/activate"

echo "==> Sweep test against $TARGET"
echo "    Test data: $AT_TESTS_ROOT"
echo "    Scanning $(ls -d "$AT_TESTS_ROOT"/*/ 2>/dev/null | wc -l) test directories"
echo

PYTHONUNBUFFERED=1 PYTHONPATH="$REPO/client/python/src" python3 - "$TARGET" "$REPO" <<'PYEOF'
import os, re, sys, time
from pathlib import Path

target = sys.argv[1]
repo   = Path(sys.argv[2])
tests  = Path(os.environ["AT_TESTS_ROOT"])

sys.path.insert(0, str(repo / "client/python/src"))
from at_runner import run_sync

MODELS = {"bellhop", "bellhop3d", "kraken", "krakenc", "bounce",
          "field", "field3d", "scooter", "sparc"}

# Wrapper functions in MATLAB that delegate to a real model
MODEL_ALIASES = {"kraken_nofield": "kraken", "kraken_rd": "kraken"}

_models_pat = "|".join(MODELS | set(MODEL_ALIASES))

# Patterns for how runtests.m invokes models:
#   bellhop( 'Ellipse' )             — MATLAB function call
#   bellhop 'Gulf_ray_ri'            — bare call with quoted arg
#   bellhop Ellipse                  — shell-style (space separated)
#   eval(['! "' runkraken '" root']) — eval shell escape for kraken
_RE_CALL  = re.compile(r"\b(" + _models_pat + r")\(\s*'([^']+)'\s*[,)]")
_RE_BARE  = re.compile(r"^[^%]*\b(" + _models_pat + r")\s+'(\w+)'", re.MULTILINE)
_RE_SHELL = re.compile(r"^[^%]*\b(" + _models_pat + r")\s+(\w+)", re.MULTILINE)
_RE_EVAL  = re.compile(
    r"""run(kraken|bellhop|scooter|krakenc|sparc|bounce)\b"""
    r""".*?['"]\s*(\w+)\s*['"]""",
    re.MULTILINE,
)

def parse_runtests(test_dir):
    """Extract (model, file_root) pairs from runtests*.m files."""
    pairs = []
    for mf in sorted(test_dir.glob("runtests*.m")):
        text = mf.read_text(errors="replace")
        for m in _RE_CALL.finditer(text):
            pairs.append((m.group(1), m.group(2)))
        for m in _RE_BARE.finditer(text):
            pairs.append((m.group(1), m.group(2)))
        for m in _RE_SHELL.finditer(text):
            pairs.append((m.group(1), m.group(2)))
        for m in _RE_EVAL.finditer(text):
            pairs.append((m.group(1), m.group(2)))

    # Resolve aliases (kraken_nofield -> kraken, etc.)
    resolved = []
    for model, root in pairs:
        resolved.append((MODEL_ALIASES.get(model, model), root))

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for p in resolved:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique

def collect_inputs(test_dir):
    inputs = {}
    for f in test_dir.iterdir():
        if f.is_file() and f.name != "Makefile" and not f.name.endswith(".m"):
            try:
                inputs[f.name] = f.read_bytes()
            except Exception:
                pass
    return inputs

passed = failed = skipped = errors = 0
results = []

def find_test_dirs(root):
    """Yield (display_name, dir_path) for each testable directory."""
    for d in sorted(root.iterdir()):
        if not d.is_dir():
            continue
        if "3D" in d.name or "3d" in d.name:
            continue
        env_files = sorted(d.glob("*.env"))
        if env_files:
            yield (d.name, d)
        else:
            for sub in sorted(d.iterdir()):
                if sub.is_dir() and list(sub.glob("*.env")):
                    yield (f"{d.name}/{sub.name}", sub)

for display_name, d in find_test_dirs(tests):
    env_files = sorted(d.glob("*.env"))

    pairs = parse_runtests(d)
    if not pairs:
        pairs = [("kraken", env_files[0].stem)]

    inputs = collect_inputs(d)

    for model, root in pairs:
        # Skip field/field3d — they need .mod from a prior run
        if model in ("field", "field3d"):
            continue

        env_name = f"{root}.env"
        if env_name not in inputs:
            continue

        t0 = time.time()
        try:
            r = run_sync(target, model=model, file_root=root,
                         inputs=inputs, timeout=120)
            dt = time.time() - t0
            ok = r.exit_code == 0
            tag = "\033[32mPASS\033[0m" if ok else "\033[33mFAIL\033[0m"
            if ok:
                passed += 1
            else:
                failed += 1
            out_names = ", ".join(sorted(r.files.keys()))
            print(f"  {tag}  {display_name:25s}  {model:10s}  {root:25s}  "
                  f"exit={r.exit_code:3d}  {dt:5.2f}s  [{out_names}]")
            results.append((display_name, model, root, r.exit_code, dt))
        except Exception as e:
            dt = time.time() - t0
            err_msg = str(e)
            if "too large" in err_msg or "OUT_OF_RANGE" in err_msg:
                import re as _re
                m = _re.search(r"(\d{6,}) bytes", err_msg)
                size_mb = int(m.group(1)) / 1e6 if m else 0
                print(f"  \033[36mSKIP\033[0m  {display_name:25s}  {model:10s}  "
                      f"{root:25s}  {dt:5.2f}s  "
                      f"output too large for RunSync ({size_mb:.0f} MB)")
                skipped += 1
            else:
                err_str = err_msg.split("\n")[0][:80]
                print(f"  \033[31mERR \033[0m  {display_name:25s}  {model:10s}  "
                      f"{root:25s}  {dt:5.2f}s  {err_str}")
                errors += 1
            results.append((display_name, model, root, -1, dt))

total = passed + failed + errors
total_time = sum(r[4] for r in results)
print(f"\n{'─'*90}")
print(f"  {passed} passed, {failed} failed, {errors} errors, "
      f"{skipped} skipped   ({total_time:.1f}s total)")
if failed == 0 and errors == 0:
    print(f"  \033[32mAll {passed} tests passed.\033[0m")
else:
    print(f"  \033[31m{failed + errors} problem(s).\033[0m")
sys.exit(1 if (failed + errors) else 0)
PYEOF
