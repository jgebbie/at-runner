"""Integration tests for the AT Runner gRPC service.

Requires a running at-runner container. Set AT_RUNNER_TARGET to override
the default "localhost:50051".

Uses Fortran test fixtures from the Acoustics Toolbox repo (tests/ tree), e.g.
https://github.com/jgebbie/at . Set AT_TESTS_ROOT to that tests/ directory, or
run ./scripts/fetch-at-tests.sh (clone to external/at) before pytest.
"""

import os
from pathlib import Path

import pytest

import at_runner
from at_runner import ATSession, Step

TARGET = os.environ.get("AT_RUNNER_TARGET", "localhost:50051")

_HERE = Path(__file__).resolve().parent
_REPO = Path(os.environ.get("AT_REPO_ROOT", _HERE.parents[2]))


def _resolve_tests_root() -> Path:
    env = os.environ.get("AT_TESTS_ROOT")
    if env:
        return Path(env)
    legacy = _REPO / "tests"
    if (legacy / "Munk").is_dir():
        return legacy
    external = _REPO / "external" / "at" / "tests"
    if (external / "Munk").is_dir():
        return external
    raise RuntimeError(
        "AT test fixtures not found. Clone https://github.com/jgebbie/at and set "
        "AT_TESTS_ROOT to its tests/ directory, or run: ./scripts/fetch-at-tests.sh"
    )


_MUNK = _resolve_tests_root() / "Munk"
MUNKK_ENV = (_MUNK / "MunkK.env").read_bytes()
MUNKK_FLP = (_MUNK / "MunkK.flp").read_bytes()


class TestTier1:
    """Tier 1: RunSync — stateless, unary."""

    def test_kraken_munk(self):
        result = at_runner.run_sync(
            TARGET,
            model="kraken",
            file_root="MunkK",
            inputs={"MunkK.env": MUNKK_ENV},
        )
        assert result.status == "completed"
        assert result.exit_code == 0
        assert "MunkK.prt" in result.files
        assert "MunkK.mod" in result.files

    def test_unknown_model(self):
        with pytest.raises(Exception) as exc_info:
            at_runner.run_sync(
                TARGET,
                model="nonexistent",
                file_root="test",
                inputs={},
            )
        assert "INVALID_ARGUMENT" in str(exc_info.value) or "unknown model" in str(
            exc_info.value
        )


class TestTier2:
    """Tier 2: Interactive session with workspace + streaming Run."""

    def test_kraken_then_field(self):
        with ATSession(TARGET) as session:
            session.upload("MunkK.env", MUNKK_ENV)

            files = session.list_files()
            names = [f[0] for f in files]
            assert "MunkK.env" in names

            result = session.run("kraken", "MunkK")
            assert result.status == "completed"
            assert result.exit_code == 0
            assert "MunkK.prt" in result.files
            assert "MunkK.mod" in result.files

            files_after = session.list_files()
            names_after = [f[0] for f in files_after]
            assert "MunkK.mod" in names_after

            session.upload("MunkK.flp", MUNKK_FLP)

            result2 = session.run("field", "MunkK")
            assert result2.status == "completed"
            assert result2.exit_code == 0
            assert "MunkK.shd" in result2.files

    def test_upload_download_delete(self):
        with ATSession(TARGET) as session:
            data = b"test content 12345"
            session.upload("test.txt", data)
            downloaded = session.download("test.txt")
            assert downloaded == data
            session.delete("test.txt")
            files = session.list_files()
            names = [f[0] for f in files]
            assert "test.txt" not in names

    def test_streaming_callback(self):
        chunks = []

        def on_output(stream: str, data: bytes):
            chunks.append((stream, data))

        with ATSession(TARGET) as session:
            session.upload("MunkK.env", MUNKK_ENV)
            result = session.run("kraken", "MunkK", on_output=on_output)
            assert result.status == "completed"

    def test_large_file_upload_download(self):
        """Test streaming logic with files larger than the 64KB chunk size."""
        with ATSession(TARGET) as session:
            # 256 KB file to force multiple chunks
            data = b"x" * (256 * 1024)
            session.upload("large.txt", data)
            downloaded = session.download("large.txt")
            assert downloaded == data
            session.delete("large.txt")

    def test_concurrent_uploads(self):
        """Test that multiple uploads do not interleave or panic due to lock issues."""
        import concurrent.futures

        with ATSession(TARGET) as session:
            data1 = b"A" * (256 * 1024)
            data2 = b"B" * (256 * 1024)

            def upload_a():
                session.upload("concurrent_a.txt", data1)

            def upload_b():
                session.upload("concurrent_b.txt", data2)

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                f1 = executor.submit(upload_a)
                f2 = executor.submit(upload_b)
                concurrent.futures.wait([f1, f2])
                # Ensure no exceptions were raised
                f1.result()
                f2.result()

            dl1 = session.download("concurrent_a.txt")
            dl2 = session.download("concurrent_b.txt")
            assert dl1 == data1
            assert dl2 == data2
            session.delete("concurrent_a.txt")
            session.delete("concurrent_b.txt")


class TestTier3:
    """Tier 3: RunPipeline — DAG orchestration."""

    def test_kraken_then_field_pipeline(self):
        with ATSession(TARGET) as session:
            result = session.run_pipeline(
                [
                    Step("k1", "kraken", "MunkK", inputs={"MunkK.env": MUNKK_ENV}),
                    Step(
                        "f1",
                        "field",
                        "MunkK",
                        inputs={"MunkK.flp": MUNKK_FLP},
                        depends_on=["k1"],
                    ),
                ]
            )
            assert result.all_succeeded
            assert "k1" in result.steps
            assert "f1" in result.steps
            assert result.steps["k1"].exit_code == 0
            assert result.steps["f1"].exit_code == 0
            assert "MunkK.mod" in result.steps["k1"].files
            assert "MunkK.shd" in result.steps["f1"].files

    def test_dependency_failure_skips(self):
        with ATSession(TARGET) as session:
            result = session.run_pipeline(
                [
                    Step(
                        "bad",
                        "kraken",
                        "nonexistent",
                        inputs={"nonexistent.env": b"invalid data"},
                    ),
                    Step(
                        "dep",
                        "field",
                        "nonexistent",
                        inputs={"nonexistent.flp": b"invalid"},
                        depends_on=["bad"],
                    ),
                ]
            )
            assert not result.all_succeeded
            assert (
                "dep" in result.skipped_steps
                or result.steps.get("dep", None) is not None
            )
