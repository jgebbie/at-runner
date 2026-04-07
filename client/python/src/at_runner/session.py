"""AT Runner client library — wraps generated gRPC stubs into a clean Python API."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Callable, Optional, Sequence

import grpc

from at_runner._generated.at.runner.v1 import runner_pb2, runner_pb2_grpc

_MAX_MSG = 256 * 1024 * 1024  # 256 MiB
_DEFAULT_CHANNEL_OPTIONS = [
    ("grpc.max_send_message_length", _MAX_MSG),
    ("grpc.max_receive_message_length", _MAX_MSG),
]


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class RunResult:
    status: str
    exit_code: int
    stdout: str
    stderr: str
    elapsed: float
    files: dict[str, bytes]


@dataclasses.dataclass
class StepResult:
    status: str
    exit_code: int
    elapsed: float
    files: dict[str, bytes]


@dataclasses.dataclass
class PipelineResult:
    all_succeeded: bool
    elapsed: float
    steps: dict[str, StepResult]
    skipped_steps: list[str]


@dataclasses.dataclass
class Step:
    id: str
    model: str
    file_root: str
    inputs: dict[str, bytes] | None = None
    depends_on: list[str] | None = None
    timeout: int | None = None


# ---------------------------------------------------------------------------
# Status enum mapping
# ---------------------------------------------------------------------------

_STATUS_MAP = {
    runner_pb2.RUN_STATUS_UNSPECIFIED: "unspecified",
    runner_pb2.RUN_STATUS_COMPLETED: "completed",
    runner_pb2.RUN_STATUS_TIMED_OUT: "timed_out",
    runner_pb2.RUN_STATUS_SIGNALED: "signaled",
    runner_pb2.RUN_STATUS_SKIPPED: "skipped",
}


def _status_str(code: int) -> str:
    return _STATUS_MAP.get(code, f"unknown({code})")


# ---------------------------------------------------------------------------
# Tier 1: run_sync (standalone function)
# ---------------------------------------------------------------------------

def run_sync(
    target: str,
    model: str,
    file_root: str,
    inputs: dict[str, bytes] | None = None,
    timeout: int | None = None,
    *,
    channel_options: list | None = None,
) -> RunResult:
    """One-shot run: send files, get files back. No session required."""
    opts = list(channel_options or []) + _DEFAULT_CHANNEL_OPTIONS
    with grpc.insecure_channel(target, options=opts) as channel:
        stub = runner_pb2_grpc.RunnerStub(channel)
        req = runner_pb2.RunSyncRequest(
            model=model,
            file_root=file_root,
            inputs=[
                runner_pb2.File(name=n, content=_to_bytes(c))
                for n, c in (inputs or {}).items()
            ],
        )
        if timeout is not None:
            req.timeout_seconds = timeout

        resp = stub.RunSync(req)

    return RunResult(
        status=_status_str(resp.status),
        exit_code=resp.exit_code,
        stdout=resp.stdout.decode("utf-8", errors="replace"),
        stderr=resp.stderr.decode("utf-8", errors="replace"),
        elapsed=resp.elapsed_seconds,
        files={f.name: f.content for f in resp.outputs},
    )


# ---------------------------------------------------------------------------
# Tier 2 + 3: ATSession
# ---------------------------------------------------------------------------

class ATSession:
    """Interactive session backed by a persistent gRPC connection."""

    def __init__(self, target: str, *, channel_options: list | None = None):
        opts = list(channel_options or []) + _DEFAULT_CHANNEL_OPTIONS
        self._channel = grpc.insecure_channel(target, options=opts)
        self._stub = runner_pb2_grpc.RunnerStub(self._channel)

    def close(self) -> None:
        self._channel.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # --- Workspace management ---

    def upload(self, name: str, content: bytes | str | Path) -> tuple[str, int]:
        """Upload a file. Returns (name, size_bytes)."""
        data = _to_bytes(content)

        def _chunks():
            chunk_size = 65536
            yield runner_pb2.FileChunk(name=name, data=data[:chunk_size])
            offset = chunk_size
            while offset < len(data):
                end = min(offset + chunk_size, len(data))
                yield runner_pb2.FileChunk(data=data[offset:end])
                offset = end

        resp = self._stub.UploadFile(_chunks())
        return resp.name, resp.size_bytes

    def download(self, name: str) -> bytes:
        """Download a file from the workspace."""
        stream = self._stub.GetFile(runner_pb2.GetFileRequest(name=name))
        buf = bytearray()
        for chunk in stream:
            buf.extend(chunk.data)
        return bytes(buf)

    def delete(self, name: str) -> None:
        self._stub.DeleteFile(runner_pb2.DeleteFileRequest(name=name))

    def list_files(self) -> list[tuple[str, int]]:
        """Returns list of (name, size_bytes)."""
        resp = self._stub.ListFiles(runner_pb2.ListFilesRequest())
        return [(f.name, f.size_bytes) for f in resp.files]

    # --- Tier 2: Run (streaming) ---

    def run(
        self,
        model: str,
        file_root: str,
        on_output: Callable[[str, bytes], None] | None = None,
        timeout: int | None = None,
    ) -> RunResult:
        """Run a model in the workspace. Blocks until complete."""
        req = runner_pb2.RunRequest(model=model, file_root=file_root)
        if timeout is not None:
            req.timeout_seconds = timeout

        stream = self._stub.Run(req)
        return _consume_run_stream(stream, on_output)

    # --- Tier 3: RunPipeline ---

    def run_pipeline(
        self,
        steps: Sequence[Step],
        on_step_output: Callable[[str, str, bytes], None] | None = None,
        timeout: int | None = None,
    ) -> PipelineResult:
        """Submit a DAG pipeline. Blocks until complete."""
        pb_steps = []
        for s in steps:
            ps = runner_pb2.PipelineStep(
                id=s.id,
                model=s.model,
                file_root=s.file_root,
                inputs=[
                    runner_pb2.File(name=n, content=_to_bytes(c))
                    for n, c in (s.inputs or {}).items()
                ],
                depends_on=s.depends_on or [],
            )
            if s.timeout is not None:
                ps.timeout_seconds = s.timeout
            pb_steps.append(ps)

        req = runner_pb2.RunPipelineRequest(steps=pb_steps)
        if timeout is not None:
            req.timeout_seconds = timeout

        stream = self._stub.RunPipeline(req)
        return _consume_pipeline_stream(stream, on_step_output)


# ---------------------------------------------------------------------------
# Internal stream consumers
# ---------------------------------------------------------------------------

def _consume_run_stream(
    stream,
    on_output: Callable[[str, bytes], None] | None,
) -> RunResult:
    stdout_parts: list[bytes] = []
    stderr_parts: list[bytes] = []
    status = "unspecified"
    exit_code = -1
    elapsed = 0.0
    files: dict[str, bytes] = {}
    current_file: str | None = None
    current_data: list[bytes] = []

    for msg in stream:
        which = msg.WhichOneof("payload")
        if which == "started":
            pass
        elif which == "output":
            chunk = msg.output
            stream_name = "stdout" if chunk.stream == runner_pb2.OUTPUT_STREAM_STDOUT else "stderr"
            if stream_name == "stdout":
                stdout_parts.append(chunk.data)
            else:
                stderr_parts.append(chunk.data)
            if on_output:
                on_output(stream_name, chunk.data)
        elif which == "completed":
            status = _status_str(msg.completed.status)
            exit_code = msg.completed.exit_code
            elapsed = msg.completed.elapsed_seconds
        elif which == "file":
            fc = msg.file
            if fc.name:
                if current_file is not None:
                    files[current_file] = b"".join(current_data)
                current_file = fc.name
                current_data = [fc.data]
            else:
                current_data.append(fc.data)

    if current_file is not None:
        files[current_file] = b"".join(current_data)

    return RunResult(
        status=status,
        exit_code=exit_code,
        stdout=b"".join(stdout_parts).decode("utf-8", errors="replace"),
        stderr=b"".join(stderr_parts).decode("utf-8", errors="replace"),
        elapsed=elapsed,
        files=files,
    )


def _consume_pipeline_stream(
    stream,
    on_step_output: Callable[[str, str, bytes], None] | None,
) -> PipelineResult:
    step_results: dict[str, dict] = {}
    all_succeeded = False
    elapsed = 0.0
    skipped_steps: list[str] = []

    # Track current file per step
    current_files: dict[str, tuple[str, list[bytes]]] = {}

    def _finalize_file(step_id: str) -> None:
        if step_id in current_files:
            fname, parts = current_files.pop(step_id)
            sr = step_results.setdefault(step_id, {"files": {}})
            sr["files"][fname] = b"".join(parts)

    for msg in stream:
        which = msg.WhichOneof("payload")
        if which == "pipeline_started":
            pass
        elif which == "step":
            ev = msg.step
            sid = ev.step_id
            detail = ev.WhichOneof("detail")
            if detail == "started":
                step_results.setdefault(sid, {"files": {}})
            elif detail == "output":
                chunk = ev.output
                stream_name = "stdout" if chunk.stream == runner_pb2.OUTPUT_STREAM_STDOUT else "stderr"
                if on_step_output:
                    on_step_output(sid, stream_name, chunk.data)
            elif detail == "completed":
                _finalize_file(sid)
                sr = step_results.setdefault(sid, {"files": {}})
                sr["status"] = _status_str(ev.completed.status)
                sr["exit_code"] = ev.completed.exit_code
                sr["elapsed"] = ev.completed.elapsed_seconds
            elif detail == "file":
                fc = ev.file
                if fc.name:
                    _finalize_file(sid)
                    current_files[sid] = (fc.name, [fc.data])
                else:
                    if sid in current_files:
                        current_files[sid][1].append(fc.data)
        elif which == "pipeline_completed":
            # Finalize any remaining files
            for sid in list(current_files):
                _finalize_file(sid)
            all_succeeded = msg.pipeline_completed.all_succeeded
            elapsed = msg.pipeline_completed.elapsed_seconds
            skipped_steps = list(msg.pipeline_completed.skipped_steps)

    steps = {}
    for sid, data in step_results.items():
        steps[sid] = StepResult(
            status=data.get("status", "unspecified"),
            exit_code=data.get("exit_code", -1),
            elapsed=data.get("elapsed", 0.0),
            files=data.get("files", {}),
        )

    return PipelineResult(
        all_succeeded=all_succeeded,
        elapsed=elapsed,
        steps=steps,
        skipped_steps=skipped_steps,
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _to_bytes(content: bytes | str | Path) -> bytes:
    if isinstance(content, bytes):
        return content
    if isinstance(content, str):
        return content.encode("utf-8")
    if isinstance(content, Path):
        return content.read_bytes()
    raise TypeError(f"expected bytes, str, or Path; got {type(content)}")
