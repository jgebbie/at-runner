"""AT Runner test driver — discovers tests from /tests, classifies them by tier,
distributes across runners, and reports results."""

from __future__ import annotations

import concurrent.futures
import json
import os
import sys
import threading
import time
from pathlib import Path

import grpc

sys.path.insert(0, "/app/src")
import at_runner
from at_runner import ATSession, Step

RUNNERS = os.environ.get("RUNNERS", "localhost:50051").split(",")
TESTS_DIR = Path(os.environ.get("TESTS_DIR", "/tests"))
TIMINGS_FILE = Path(os.environ.get("TIMINGS_FILE", "/tmp/timings.json"))

# Model executables and the programs they map to
MULTI_MODEL_KEYWORDS = {"field", "field3d"}
MODEL_EXTENSIONS = {
    "bellhop": ".env",
    "bellhop3d": ".env",
    "kraken": ".env",
    "krakenc": ".env",
    "bounce": ".env",
    "field": ".flp",
    "field3d": ".flp",
    "scooter": ".env",
    "sparc": ".env",
}


def discover_tests() -> list[dict]:
    """Scan /tests for test cases. Each directory with a .env file is a test case."""
    tests = []
    if not TESTS_DIR.exists():
        print(f"Tests directory {TESTS_DIR} not found")
        return tests

    for d in sorted(TESTS_DIR.iterdir()):
        if not d.is_dir():
            continue
        env_files = list(d.glob("*.env"))
        if not env_files:
            continue

        makefile = d / "Makefile"
        models = detect_models(d, makefile)
        if not models:
            continue

        total_size = sum(f.stat().st_size for f in d.iterdir() if f.is_file())

        tests.append({
            "name": d.name,
            "path": str(d),
            "models": models,
            "total_size": total_size,
            "tier": classify_tier(models, total_size),
        })

    return tests


def detect_models(test_dir: Path, makefile: Path) -> list[str]:
    """Determine which models to run from the Makefile or file contents."""
    models = []
    if makefile.exists():
        text = makefile.read_text(errors="replace").lower()
        for model in MODEL_EXTENSIONS:
            if f"{model}.exe" in text or f"{model} " in text:
                models.append(model)

    if not models:
        if list(test_dir.glob("*.flp")):
            models = ["kraken", "field"]
        elif list(test_dir.glob("*.env")):
            models = ["kraken"]

    return models


def classify_tier(models: list[str], total_size: int) -> int:
    """Assign test to an API tier based on complexity."""
    if len(models) > 1 or any(m in MULTI_MODEL_KEYWORDS for m in models):
        return 3
    if total_size > 1_000_000:
        return 2
    return 1


def run_tier1_test(runner: str, test: dict) -> dict:
    """Run a single-model test via RunSync."""
    test_dir = Path(test["path"])
    model = test["models"][0]
    env_files = list(test_dir.glob("*.env"))
    if not env_files:
        return {"name": test["name"], "status": "skip", "reason": "no .env file"}

    file_root = env_files[0].stem
    inputs = {}
    for f in test_dir.iterdir():
        if f.is_file():
            inputs[f.name] = f.read_bytes()

    start = time.time()
    try:
        result = at_runner.run_sync(
            runner,
            model=model,
            file_root=file_root,
            inputs=inputs,
            timeout=120,
        )
        elapsed = time.time() - start
        return {
            "name": test["name"],
            "tier": 1,
            "status": "pass" if result.exit_code == 0 else "fail",
            "exit_code": result.exit_code,
            "elapsed": elapsed,
            "outputs": list(result.files.keys()),
        }
    except Exception as e:
        return {
            "name": test["name"],
            "tier": 1,
            "status": "error",
            "error": str(e),
            "elapsed": time.time() - start,
        }


def run_tier2_test(runner: str, test: dict) -> dict:
    """Run a single-model test via workspace + Run."""
    test_dir = Path(test["path"])
    model = test["models"][0]
    env_files = list(test_dir.glob("*.env"))
    if not env_files:
        return {"name": test["name"], "status": "skip", "reason": "no .env file"}

    file_root = env_files[0].stem
    start = time.time()
    try:
        with ATSession(runner) as session:
            for f in test_dir.iterdir():
                if f.is_file():
                    session.upload(f.name, f.read_bytes())

            result = session.run(model, file_root, timeout=120)
            elapsed = time.time() - start
            return {
                "name": test["name"],
                "tier": 2,
                "status": "pass" if result.exit_code == 0 else "fail",
                "exit_code": result.exit_code,
                "elapsed": elapsed,
                "outputs": list(result.files.keys()),
            }
    except Exception as e:
        return {
            "name": test["name"],
            "tier": 2,
            "status": "error",
            "error": str(e),
            "elapsed": time.time() - start,
        }


def run_tier3_test(runner: str, test: dict) -> dict:
    """Run a multi-model test via RunPipeline."""
    test_dir = Path(test["path"])
    models = test["models"]
    env_files = list(test_dir.glob("*.env"))
    if not env_files:
        return {"name": test["name"], "status": "skip", "reason": "no .env file"}

    file_root = env_files[0].stem
    start = time.time()
    try:
        inputs_all = {}
        for f in test_dir.iterdir():
            if f.is_file():
                inputs_all[f.name] = f.read_bytes()

        steps = []
        prev_id = None
        for i, model in enumerate(models):
            step_id = f"{model}_{i}"
            step_inputs = {}
            for fname, data in inputs_all.items():
                ext = MODEL_EXTENSIONS.get(model, ".env")
                if fname.endswith(ext) or (i == 0):
                    step_inputs[fname] = data

            step = Step(
                id=step_id,
                model=model,
                file_root=file_root,
                inputs=step_inputs if i == 0 else {
                    k: v for k, v in inputs_all.items()
                    if k.endswith(MODEL_EXTENSIONS.get(model, ""))
                },
                depends_on=[prev_id] if prev_id else [],
            )
            steps.append(step)
            prev_id = step_id

        with ATSession(runner) as session:
            result = session.run_pipeline(steps, timeout=300)
            elapsed = time.time() - start
            return {
                "name": test["name"],
                "tier": 3,
                "status": "pass" if result.all_succeeded else "fail",
                "elapsed": elapsed,
                "steps": {
                    sid: {"exit_code": sr.exit_code, "outputs": list(sr.files.keys())}
                    for sid, sr in result.steps.items()
                },
            }
    except Exception as e:
        return {
            "name": test["name"],
            "tier": 3,
            "status": "error",
            "error": str(e),
            "elapsed": time.time() - start,
        }


def main():
    tests = discover_tests()
    if not tests:
        print("No tests discovered")
        sys.exit(0)

    print(f"Discovered {len(tests)} test cases")
    for t in tests:
        print(f"  [{t['tier']}] {t['name']}: {', '.join(t['models'])}")

    # Load timings for load balancing
    timings = {}
    if TIMINGS_FILE.exists():
        try:
            timings = json.loads(TIMINGS_FILE.read_text())
        except Exception:
            pass

    # Round-robin assignment (or by estimated cost if timings available)
    assignments: dict[str, list[dict]] = {r: [] for r in RUNNERS}
    loads = {r: 0.0 for r in RUNNERS}

    for test in sorted(tests, key=lambda t: timings.get(t["name"], 0), reverse=True):
        runner = min(loads, key=loads.get)
        assignments[runner].append(test)
        loads[runner] += timings.get(test["name"], 10.0)

    results = []

    print_lock = threading.Lock()

    def runner_worker(runner: str, runner_tests: list[dict]) -> list[dict]:
        local_results = []
        with print_lock:
            print(f"\nRunner {runner}: {len(runner_tests)} tests")
        for test in runner_tests:
            with print_lock:
                print(
                    f"  Running [{test['tier']}] {test['name']}...",
                    end=" ",
                    flush=True,
                )
            if test["tier"] == 1:
                r = run_tier1_test(runner, test)
            elif test["tier"] == 2:
                r = run_tier2_test(runner, test)
            else:
                r = run_tier3_test(runner, test)
            with print_lock:
                print(f"{r['status']} ({r.get('elapsed', 0):.1f}s)")
            local_results.append(r)
        return local_results

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(RUNNERS) or 1) as ex:
        futures = []
        for runner, runner_tests in assignments.items():
            futures.append(ex.submit(runner_worker, runner, runner_tests))
        for fut in concurrent.futures.as_completed(futures):
            results.extend(fut.result())

    # Save timings
    new_timings = {}
    for r in results:
        if "elapsed" in r:
            new_timings[r["name"]] = r["elapsed"]
    try:
        TIMINGS_FILE.write_text(json.dumps(new_timings, indent=2))
    except Exception:
        pass

    # Report
    passed = sum(1 for r in results if r["status"] == "pass")
    failed = sum(1 for r in results if r["status"] == "fail")
    errors = sum(1 for r in results if r["status"] == "error")
    skipped = sum(1 for r in results if r["status"] == "skip")

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {errors} errors, {skipped} skipped")
    print(f"{'='*60}")

    if failed > 0 or errors > 0:
        print("\nFailures:")
        for r in results:
            if r["status"] in ("fail", "error"):
                fallback = f"exit_code={r.get('exit_code')}"
                print(f"  {r['name']}: {r['status']} - {r.get('error', fallback)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
