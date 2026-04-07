# AT Runner — Architecture

For a short overview, repository layout, and commands to build and test, see [README.md](README.md).

The Acoustics Toolbox (AT) is a suite of Fortran programs for modeling underwater sound propagation. Each program implements a different computational approach — ray tracing, normal modes, wavenumber integration — and they chain together: a normal-mode solver produces mode files that a field computation program consumes. Natively, users run these programs from the command line in a shared working directory, preparing text input files and collecting binary output files by hand.

The AT Runner service wraps these executables in a gRPC server, exposing them over the network. Clients upload input files, invoke models, and retrieve results without installing Fortran compilers or managing local builds. The session workspace preserves the directory-based chaining that the underlying programs expect.

## System Overview

```
 ┌──────────────────────────────────────────────────────────────┐
 │ Client (Python, MATLAB, Rust, etc.)                          │
 │                                                              │
 │  ┌────────────────────────────────────────────────────────┐  │
 │  │ User Code / Jupyter Notebook                           │  │
 │  │                                                        │  │
 │  │   session = ATSession("host:50051")                    │  │
 │  │   session.upload("Munk.env", env_data)                 │  │
 │  │   result = session.run("kraken", "Munk")               │  │
 │  │   print(result.stdout)                                 │  │
 │  │   shd = result.files["Munk.shd"]                       │  │
 │  └──────────────────────┬─────────────────────────────────┘  │
 │                         │                                    │
 │  ┌──────────────────────▼─────────────────────────────────┐  │
 │  │ Client Library (e.g. at_runner Python package)         │  │
 │  │ Handles chunk assembly, stream consumption,            │  │
 │  │ optional real-time callbacks, result packaging.        │  │
 │  └──────────────────────┬─────────────────────────────────┘  │
 │                         │                                    │
 │  ┌──────────────────────▼─────────────────────────────────┐  │
 │  │ gRPC Generated Stubs (protoc output)                   │  │
 │  │ Raw streaming interface — available for advanced use.   │  │
 │  └──────────────────────┬─────────────────────────────────┘  │
 └─────────────────────────┼────────────────────────────────────┘
                           │ gRPC / HTTP/2
 ┌─────────────────────────┼────────────────────────────────────┐
 │ Server (Docker Container)                                    │
 │                         │                                    │
 │  ┌──────────────────────▼─────────────────────────────────┐  │
 │  │ at-runner gRPC Server (Rust / tonic)                   │  │
 │  │ RunSync | Run | RunPipeline | Workspace RPCs | Health  │  │
 │  └──────────┬─────────────────────┬───────────────────────┘  │
 │             │                     │                          │
 │  ┌──────────▼──────────┐  ┌──────▼────────────────────┐     │
 │  │ Workspace Manager   │  │ Process Executor           │     │
 │  │ /workspace/ (tmpfs) │  │ tokio::process::Command    │     │
 │  │                     │  │ pipe streaming + timeout   │     │
 │  └─────────────────────┘  └──────┬────────────────────┘     │
 │                                  │                           │
 │                           ┌──────▼──────────────────┐        │
 │                           │ AT Fortran Executables   │        │
 │                           │ /at/bin/                 │        │
 │                           │ bellhop.exe, kraken.exe  │        │
 │                           │ krakenc.exe, field.exe   │        │
 │                           │ scooter.exe, sparc.exe   │        │
 │                           │ bounce.exe, ...          │        │
 │                           └─────────────────────────┘        │
 └──────────────────────────────────────────────────────────────┘
```

## Layers

| Layer | Name | Role |
|-------|------|------|
| **gRPC wire protocol** | `at.runner.v1` proto | Three-tiered API: simple unary, streaming interactive, and DAG orchestration. Not designed for direct human use. |
| **Client library** | `at_runner` (Python package, etc.) | Friendly wrapper over the generated stubs. Hides chunking, stream consumption, and gRPC ceremony. One per target language. |
| **Server** | `at-runner` (Rust binary) | Executes AT models in a container, manages the session workspace, orchestrates pipelines. |

## Supported Executables

Built by `make install` into `bin/`:

| Model | Executable | Purpose | Typical Inputs | Typical Outputs |
|-------|-----------|---------|----------------|-----------------|
| bellhop | `bellhop.exe` | Ray/beam tracing for acoustic propagation | `.env`, `.bty`, `.ati`, `.ssp`, `.trc`, `.brc`, `.sbp` | `.prt`, `.ray`, `.shd`, `.arr` |
| bellhop3d | `bellhop3d.exe` | 3D ray/beam tracing | `.env`, `.bty`, `.ati`, `.ssp`, `.trc`, `.brc`, `.sbp` | `.prt`, `.ray`, `.shd`, `.arr` |
| kraken | `kraken.exe` | Normal-mode solver for range-independent environments | `.env` | `.prt`, `.mod` |
| krakenc | `krakenc.exe` | Complex normal-mode solver (lossy environments) | `.env` | `.prt`, `.mod` |
| bounce | `bounce.exe` | Reflection coefficient computation | `.env`, `.brc`, `.trc` | `.prt`, `.irc`, `.brc` |
| field | `field.exe` | Pressure field from mode summation (uses kraken output) | `.flp`, `.mod` | `.prt`, `.shd` |
| field3d | `field3d.exe` | 3D pressure field from mode summation | `.flp`, `.mod` | `.prt`, `.shd` |
| scooter | `scooter.exe` | Wavenumber integration (Green's function) | `.env` | `.prt`, `.grn` |
| sparc | `sparc.exe` | Time-domain wavenumber integration | `.env` | `.prt`, `.grn`, `.rts` |

All programs take a **file root** (e.g. `MunkB_ray`) as their first command-line argument and derive filenames by appending extensions (e.g. `MunkB_ray.env`, `MunkB_ray.prt`).

## File Type Classification

The service transmits all file contents as raw bytes, but this classification matters for debugging and for any future semantic API.

**Binary** (direct-access unformatted — must be transferred as opaque bytes):
- `.mod` — mode files (Kraken/Krakenc)
- `.shd` — shade/pressure field files
- `.grn` — Green's function files
- `.arr` — arrivals (binary when Bellhop run type selects unformatted)

**Text** (sequential formatted):
- `.env`, `.flp`, `.ssp`, `.bty`, `.ati`, `.brc`, `.trc`, `.irc`, `.sbp`, `.prt`, `.ray`, `.rts`
- `.arr` — arrivals (text when Bellhop run type selects formatted)

The `.arr` extension is dual-mode (text or binary depending on Bellhop options). The low-level service treats everything as bytes and does not need to distinguish.

## Session Model

Each container runs a single at-runner instance and serves one client session. The container IS the session — there is **no multiplexing** of workspaces and **no session identifier in the gRPC API** (clients do not pass or receive a session ID).

For **operations and debugging**, the server assigns a **per-RPC correlation ID** (UUID) to log lines: field `session_id` in structured [`tracing`](https://docs.rs/tracing) output. That ID is only for log correlation; it is unrelated to the workspace session above. Pipeline steps additionally log subprocess scope as `{pipeline_session_id}:step:{step_id}`.

A **persistent workspace directory** (`/workspace`, tmpfs-backed) holds session state:
- Files uploaded by the client persist across runs.
- Output files from one run (e.g. `.mod` from kraken) are automatically available as inputs to the next (e.g. `field.exe`).
- Large files like bathymetry are uploaded once and reused for many runs.
- The workspace is destroyed when the container stops.

This matches how users work with AT natively: run kraken to produce mode files, then run field which reads them from the same directory.

## gRPC API Definition

The API has three tiers. A developer picks the tier that matches their needs. All tiers share common message types so they feel like one cohesive API.

| Tier | RPCs | Audience | Session state | Streaming | Parallel |
|------|------|----------|---------------|-----------|----------|
| 1. Simple | `RunSync` | Quick scripts, CI, testing | Read-only | No | No |
| 2. Interactive | `Run`, workspace RPCs | Jupyter, large files, long runs | Read/write | Yes | No |
| 3. Orchestration | `RunPipeline` | Multi-model workflows, sweeps | Read-only | Yes | Yes |

```protobuf
syntax = "proto3";
package at.runner.v1;

service Runner {
  // --- Tier 1: Simple (unary, self-contained) ---
  rpc RunSync(RunSyncRequest) returns (RunSyncResponse);

  // --- Tier 2: Interactive (streaming, session workspace) ---
  rpc UploadFile(stream FileChunk) returns (UploadResponse);
  rpc GetFile(GetFileRequest) returns (stream FileChunk);
  rpc DeleteFile(DeleteFileRequest) returns (DeleteFileResponse);
  rpc ListFiles(ListFilesRequest) returns (ListFilesResponse);
  rpc Run(RunRequest) returns (stream RunOutput);

  // --- Tier 3: Orchestration (DAG of runs, parallel execution) ---
  rpc RunPipeline(RunPipelineRequest) returns (stream PipelineOutput);
}

// ============================================================
// Shared types
// ============================================================

message File {
  string name = 1;    // filename only, no path
  bytes content = 2;
}

message FileChunk {
  string name = 1;    // set in first chunk of each file; empty in continuations
  bytes data = 2;
}

message FileInfo {
  string name = 1;
  uint64 size_bytes = 2;
}

enum OutputStream {
  OUTPUT_STREAM_UNSPECIFIED = 0;
  OUTPUT_STREAM_STDOUT = 1;
  OUTPUT_STREAM_STDERR = 2;
}

enum RunStatus {
  RUN_STATUS_UNSPECIFIED = 0;
  RUN_STATUS_COMPLETED = 1;
  RUN_STATUS_TIMED_OUT = 2;
  RUN_STATUS_SIGNALED = 3;
  RUN_STATUS_SKIPPED = 4;    // pipeline only: dependency failed
}

message OutputChunk {
  OutputStream stream = 1;
  bytes data = 2;
}

message RunStarted {
  string model = 1;
  string file_root = 2;
}

message RunCompleted {
  RunStatus status = 1;
  int32 exit_code = 2;
  double elapsed_seconds = 3;
  repeated FileInfo output_files = 4;
}

// ============================================================
// Tier 1: RunSync — send files, get files back
// ============================================================

message RunSyncRequest {
  string model = 1;
  string file_root = 2;
  repeated File inputs = 3;
  optional uint32 timeout_seconds = 4;
}

message RunSyncResponse {
  RunStatus status = 1;
  int32 exit_code = 2;
  bytes stdout = 3;
  bytes stderr = 4;
  double elapsed_seconds = 5;
  repeated File outputs = 6;
}

// ============================================================
// Tier 2: Workspace management
// ============================================================

message UploadResponse {
  string name = 1;
  uint64 size_bytes = 2;
}

message GetFileRequest {
  string name = 1;
}

message DeleteFileRequest {
  string name = 1;
}

message DeleteFileResponse {}

message ListFilesRequest {}

message ListFilesResponse {
  repeated FileInfo files = 1;
}

// ============================================================
// Tier 2: Run — streaming execution in the workspace
// ============================================================

message RunRequest {
  string model = 1;
  string file_root = 2;
  optional uint32 timeout_seconds = 3;
}

// Streamed response — four phases in order:
//   1. started     — confirms the run launched
//   2. output      — real-time stdout/stderr chunks (interleaved)
//   3. completed   — exit code, timing, manifest of output files
//   4. file        — output file contents, chunked
message RunOutput {
  oneof payload {
    RunStarted started = 1;
    OutputChunk output = 2;
    RunCompleted completed = 3;
    FileChunk file = 4;
  }
}

// ============================================================
// Tier 3: RunPipeline — DAG of runs with parallel execution
// ============================================================

message RunPipelineRequest {
  repeated PipelineStep steps = 1;
  optional uint32 timeout_seconds = 2;  // overall pipeline timeout
}

message PipelineStep {
  string id = 1;                        // unique within this pipeline
  string model = 2;                     // e.g. "kraken"
  string file_root = 3;                 // passed as argv[1]
  repeated File inputs = 4;             // files specific to this step
  repeated string depends_on = 5;       // step ids that must complete first
  optional uint32 timeout_seconds = 6;  // per-step timeout
}

// Streamed response — interleaved events from all steps.
message PipelineOutput {
  oneof payload {
    PipelineStarted pipeline_started = 1;
    StepEvent step = 2;
    PipelineCompleted pipeline_completed = 3;
  }
}

message PipelineStarted {
  repeated string step_ids = 1;
}

// Wraps existing run-level messages with a step_id.
message StepEvent {
  string step_id = 1;
  oneof detail {
    RunStarted started = 2;
    OutputChunk output = 3;
    RunCompleted completed = 4;
    FileChunk file = 5;
  }
}

message PipelineCompleted {
  bool all_succeeded = 1;
  double elapsed_seconds = 2;
  repeated string skipped_steps = 3;  // step ids skipped due to dependency failure
}
```

### Design Decisions

**Three tiers, one service.** Rather than separate gRPC services, all RPCs live in one `Runner` service. They share message types (`RunStatus`, `RunCompleted`, `OutputChunk`, `FileChunk`) and differ only in how much machinery the caller engages. A script that calls `RunSync` never encounters streaming, workspace, or DAG concepts. A scientist in Jupyter uses `Run` and the workspace. An automated workflow uses `RunPipeline`.

**Why `RunSync` exists alongside `Run`.** `RunSync` is the "curl-equivalent" -- usable from any language with a gRPC stub, no wrapper library needed, no session setup. It's a single request/response: send files, get files back. The trade-off is explicit: no real-time stdout, no streaming for large outputs, message-size limits apply (gRPC default 4 MB, configurable). For the vast majority of AT runs where files are KB to low MB, this covers the use case with minimal ceremony.

**`RunSync` has read-only workspace access.** Even though `RunSync` is "simple," it can see files already in the session workspace. This means a pre-uploaded bathymetry file is available to `RunSync` calls without re-sending it. The execution creates a temp directory with symlinks to workspace files plus the inline inputs. Outputs go only to the response, never to the workspace. From the client's perspective, `RunSync` behaves statelessly -- but it benefits from session state when it exists.

**Why `file_root` is explicit.** All AT programs read `FileRoot` from `GET_COMMAND_ARGUMENT(1)`. Making this explicit avoids fragile heuristics (like guessing the root from the `.env` filename) and supports cases where the caller provides files with different roots (e.g. `field.exe` reading mode files produced by a prior `kraken.exe` run).

**Why `bytes` throughout.** Fortran formatted output can contain characters outside valid UTF-8 in edge cases. File contents may be binary. Using `bytes` for stdout, stderr, and file data avoids protobuf string validation failures. Callers decode as UTF-8 with lossy conversion where appropriate.

**Pipeline dependencies are step-level, not file-level.** `depends_on` references step IDs, meaning all output files from the depended-on step are available. There is no per-file routing. AT workflows almost always need "everything run A produced" -- specifying individual files would add proto complexity without practical benefit.

**Pipeline steps run in isolated directories.** Each step gets its own subdirectory containing: (1) symlinks to workspace files (shared bathymetry, etc.), (2) symlinks to output files from dependency steps, (3) the step's inline input files (override symlinks if same name). This prevents file collisions between parallel steps and keeps the workspace clean.

**Pipeline failure policy is best-effort.** When a step fails (nonzero exit, timeout, or signal), the server does not cancel independent steps. Steps whose `depends_on` includes the failed step are skipped — they receive a `StepEvent` with `RUN_STATUS_SKIPPED` and never execute. Independent branches continue. This maximizes useful output from a single pipeline submission. `PipelineCompleted.all_succeeded` is false if any step failed or was skipped, and `skipped_steps` lists which steps were not executed.

**Pipeline outputs do not persist to the workspace.** The pipeline streams results back and cleans up its temp directories. If the client wants to keep a file for future use, it re-uploads via `UploadFile`. This avoids polluting the workspace with intermediate files from every pipeline run.

### Streaming Rationale

| RPC | Pattern | Why |
|-----|---------|-----|
| `RunSync` | Unary | Simplicity. Entire request and response fit in memory. No streaming complexity. |
| `UploadFile` | Client-streaming | Large files (bathymetry) sent in chunks. |
| `GetFile` | Server-streaming | Large output files (`.shd`) sent in chunks. |
| `Run` | Server-streaming | Real-time stdout/stderr during execution, then output files chunked. |
| `RunPipeline` | Server-streaming | Interleaved step events from parallel runs, then output files per step. |
| `ListFiles` | Unary | Small payload. |
| `DeleteFile` | Unary | Small payload. |

Default chunk size: 64 KB. Configurable via server CLI flag.

## Tier 1: RunSync Lifecycle

```
Client                            at-runner                         Filesystem
  |                                  |                                  |
  |-- RunSyncRequest(kraken, ...)-->|                                  |
  |                                  |-- mkdir /tmp/xxxx -------------->|
  |                                  |-- symlink workspace files ------>|  (read-only)
  |                                  |-- write inline inputs ---------->|
  |                                  |-- kraken.exe pekeris ----------->|
  |                                  |<-- exit code 0 -----------------|
  |                                  |-- read output files ------------>|
  |                                  |-- rmdir /tmp/xxxx -------------->|
  |                                  |                                  |
  |<-- RunSyncResponse(0, files) --|   (workspace unchanged)           |
```

One request, one response. Workspace files are visible but not modified.

## Tier 2: Run Stream Lifecycle

```
Client                        at-runner                         Workspace (tmpfs)
  |                              |                                  |
  |--- RunRequest(bellhop) ----->|                                  |
  |                              |-- snapshot workspace ----------->|
  |                              |-- spawn bellhop.exe MunkB_ray -->|
  |                              |                                  |
  |<-- RunStarted --------------|                                  |
  |                              |                                  |
  |<-- OutputChunk(stdout) -----|<-- stdout data ------------------|
  |<-- OutputChunk(stdout) -----|<-- stdout data ------------------|
  |<-- OutputChunk(stderr) -----|<-- stderr data ------------------|
  |<-- OutputChunk(stdout) -----|<-- stdout data ------------------|
  |                              |                                  |
  |                              |<-- process exits (code 0) ------|
  |                              |-- diff workspace vs snapshot --->|
  |                              |                                  |
  |<-- RunCompleted(0, files) --|                                  |
  |<-- FileChunk(MunkB.prt) ----|-- read MunkB_ray.prt ----------->|
  |<-- FileChunk(MunkB.shd) ----|-- read MunkB_ray.shd chunk 1 -->|
  |<-- FileChunk(MunkB.shd) ----|-- read MunkB_ray.shd chunk 2 -->|
  |                              |                                  |
  |    (stream completes)        |   (files persist in workspace)   |
```

## Tier 2: Typical Multi-Run Session

```
Client                           at-runner
  |                                 |
  |-- UploadFile(MunkB.env) ------->|  workspace: [MunkB.env]
  |-- UploadFile(MunkB.bty) ------->|  workspace: [MunkB.env, MunkB.bty]
  |                                 |
  |-- Run(kraken, MunkB) ---------->|  produces MunkB.prt, MunkB.mod
  |<-- stdout/stderr (streamed) ----|
  |<-- RunCompleted + files --------|  workspace: [MunkB.env, MunkB.bty, MunkB.prt, MunkB.mod]
  |                                 |
  |-- UploadFile(MunkB.flp) ------->|  workspace: [... + MunkB.flp]
  |                                 |
  |-- Run(field, MunkB) ----------->|  reads MunkB.flp + MunkB.mod (already in workspace)
  |<-- stdout/stderr (streamed) ----|
  |<-- RunCompleted + files --------|  workspace: [... + MunkB.shd]
  |                                 |
  |-- UploadFile(MunkB2.env) ------>|  different parameters, same bathymetry
  |-- Run(kraken, MunkB2) --------->|  MunkB.bty still in workspace
  |<-- ... ------------------------|
```

## Tier 3: Pipeline Execution

Each step runs in its own directory. Independent steps run in parallel. Outputs from dependency steps are symlinked into dependent steps' directories.

```
Client                                at-runner
  |                                      |
  |  (workspace already has Munk.bty)    |
  |                                      |
  |-- RunPipelineRequest --------------->|
  |     step "k1": kraken, freq1         |
  |       inputs: [freq1.env]            |
  |     step "k2": kraken, freq2         |
  |       inputs: [freq2.env]            |
  |     step "f1": field, freq1          |
  |       inputs: [freq1.flp]            |
  |       depends_on: ["k1"]             |
  |     step "f2": field, freq2          |
  |       inputs: [freq2.flp]            |
  |       depends_on: ["k2"]             |
  |                                      |
  |<-- PipelineStarted [k1,k2,f1,f2] ---|
  |                                      |-- mkdir steps/k1, steps/k2
  |                                      |-- symlink Munk.bty into each
  |                                      |-- write freq1.env, freq2.env
  |                                      |
  |                                      |-- spawn kraken.exe freq1  ─┐
  |                                      |-- spawn kraken.exe freq2  ─┤ parallel
  |<-- StepEvent(k1, started) ----------|                             │
  |<-- StepEvent(k2, started) ----------|                             │
  |<-- StepEvent(k1, output: stdout) ---|                             │
  |<-- StepEvent(k2, output: stdout) ---|                             │
  |<-- StepEvent(k1, completed: 0) -----|  k1 done                   │
  |<-- StepEvent(k2, completed: 0) -----|  k2 done ──────────────────┘
  |                                      |
  |                                      |-- mkdir steps/f1, steps/f2
  |                                      |-- symlink Munk.bty into each
  |                                      |-- symlink k1 outputs → f1
  |                                      |-- symlink k2 outputs → f2
  |                                      |-- write freq1.flp, freq2.flp
  |                                      |
  |                                      |-- spawn field.exe freq1  ──┐
  |                                      |-- spawn field.exe freq2  ──┤ parallel
  |<-- StepEvent(f1, started) ----------|                             │
  |<-- StepEvent(f2, started) ----------|                             │
  |<-- StepEvent(f1, completed: 0) -----|                             │
  |<-- StepEvent(f2, completed: 0) -----|─────────────────────────────┘
  |                                      |
  |<-- StepEvent(f1, file: freq1.shd) --|  output files streamed
  |<-- StepEvent(f2, file: freq2.shd) --|
  |<-- PipelineCompleted(ok, 3.5s) -----|
  |                                      |-- cleanup temp dirs
```

**Key properties:**
- Steps with no unmet dependencies start immediately (k1 and k2 run in parallel).
- When a step completes, any step whose `depends_on` list is now fully satisfied starts.
- Outputs from completed steps are symlinked into dependent steps' directories before they launch.
- Workspace files (Munk.bty) are available to all steps as a read-only base layer.
- Intermediate files (freq1.mod) flow between steps without leaving the server.
- The workspace itself is not modified by the pipeline.

## Container-per-Session Deployment

Each container is a single session. For multiple concurrent users, spawn multiple containers.

```
                     ┌──────────────────────┐
                     │  Gateway / Orch.     │
                     │  (future, optional)  │
                     └──────────┬───────────┘
                                │ create/route/destroy
                ┌───────────────┼───────────────┐
                │               │               │
         ┌──────▼──────┐ ┌─────▼───────┐ ┌─────▼───────┐
         │ Session A    │ │ Session B    │ │ Session C    │
         │ at-runner    │ │ at-runner    │ │ at-runner    │
         │ :50051       │ │ :50051       │ │ :50051       │
         │              │ │              │ │              │
         │ /workspace   │ │ /workspace   │ │ /workspace   │
         │ (tmpfs)      │ │ (tmpfs)      │ │ (tmpfs)      │
         └──────────────┘ └──────────────┘ └──────────────┘
```

**Day-one deployment** (single user): one container, connect directly.

```bash
docker run -p 50051:50051 --tmpfs /workspace:rw,noexec,nosuid,size=512m at-runner
```

**Multi-user deployment**: multiple containers behind a gateway. See the Future Improvements section for details.

The at-runner service itself does not change — it always serves exactly one session.

## Client Libraries

Client libraries are provided in Python and Rust. Both expose the same three-tier API surface and hide the gRPC streaming ceremony. The raw generated stubs remain available for advanced use in any language.

### Why wrappers are needed

gRPC streaming is the right wire protocol, but gRPC does **not** auto-stitch streaming chunks. The generated Python stubs expose a raw iterator of protobuf messages that the caller must dispatch on `oneof` variant, track current-file state, and concatenate byte chunks manually. This is protocol ceremony that should be hidden from end users.

The client library is **additive, not alternative**. The raw gRPC stubs remain available for advanced use (custom streaming callbacks, cancellation, etc.). The wrapper consumes them and exposes clean Python methods.

### Tier 1: `run_sync` — no session, one function call

For quick integration, testing, or scripting. No session setup, no streaming, no workspace management. Everything in one call.

```python
import at_runner

result = at_runner.run_sync(
    "localhost:50051",
    model="kraken",
    file_root="pekeris",
    inputs={"pekeris.env": env_data},
)

result.status       # "completed"
result.exit_code    # 0
result.stdout       # str
result.stderr       # str
result.files        # {"pekeris.prt": b"...", "pekeris.mod": b"..."}
```

This is the lowest barrier to entry. A developer unfamiliar with gRPC or AT internals can call this from any Python script.

### Tier 2: `ATSession` — interactive exploration

For Jupyter, long-running models, large shared files, and multi-step workflows.

```python
from at_runner import ATSession

session = ATSession("localhost:50051")

# Upload files (accepts bytes, str, or a file path)
session.upload("MunkB_ray.env", env_content)
session.upload("MunkB_ray.bty", Path("MunkB_ray.bty"))

# Run a model — blocks until complete, returns assembled result
result = session.run("kraken", "MunkB_ray")

# Output files from kraken are in the workspace for the next run
session.upload("MunkB_ray.flp", flp_content)
result = session.run("field", "MunkB_ray")
shd_data = result.files["MunkB_ray.shd"]

# Workspace management
session.list_files()              # -> list of (name, size) tuples
session.download("MunkB_ray.shd") # -> bytes
session.delete("MunkB_ray.prt")
```

**Real-time output** — for long-running models, pass a callback. Invoked from the same thread, no multithreading required:

```python
def on_output(stream: str, data: bytes):
    print(data.decode("utf-8", errors="replace"), end="", flush=True)

result = session.run("bellhop3d", "BigCase", on_output=on_output, timeout=600)
```

Without the callback, `session.run()` silently consumes the stream and assembles the result.

### Tier 3: `run_pipeline` — DAG orchestration

For multi-model workflows with dependencies and parallelism. Upload shared files to the workspace first, then submit the pipeline.

```python
from at_runner import ATSession, Step

session = ATSession("localhost:50051")
session.upload("Munk.bty", bty_data)   # shared bathymetry, uploaded once

result = session.run_pipeline([
    Step("k1", "kraken", "freq1", inputs={"freq1.env": env1}),
    Step("k2", "kraken", "freq2", inputs={"freq2.env": env2}),
    Step("f1", "field",  "freq1", inputs={"freq1.flp": flp1}, depends_on=["k1"]),
    Step("f2", "field",  "freq2", inputs={"freq2.flp": flp2}, depends_on=["k2"]),
])

result.all_succeeded    # True
result.elapsed          # 3.5 (seconds — k1 and k2 ran in parallel)
result.steps["k1"].exit_code        # 0
result.steps["f1"].files["freq1.shd"]  # bytes
```

The client library consumes the interleaved `PipelineOutput` stream and assembles per-step results. No multithreading on the client side — the server handles parallel execution.

**With real-time output per step:**

```python
def on_step_output(step_id: str, stream: str, data: bytes):
    print(f"[{step_id}] {data.decode('utf-8', errors='replace')}", end="")

result = session.run_pipeline(steps, on_step_output=on_step_output)
```

### Rust client library

The Rust client library (`client/rust/`) provides the same three-tier API, idiomatic to Rust:

```rust
// Tier 1 — blocking, no session
let result = at_runner::run_sync("localhost:50051", "kraken", "pekeris",
    &[("pekeris.env", &env_data)])?;
println!("{}", result.stdout);

// Tier 2 — async session
let session = ATSession::connect("localhost:50051").await?;
session.upload("Munk.env", &env_data).await?;
let result = session.run("kraken", "Munk").await?;

// Tier 3 — pipeline
let result = session.run_pipeline(&[
    Step::new("k1", "kraken", "freq1").with_input("freq1.env", &env1),
    Step::new("k2", "kraken", "freq2").with_input("freq2.env", &env2),
    Step::new("f1", "field", "freq1").with_input("freq1.flp", &flp1).depends_on(&["k1"]),
    Step::new("f2", "field", "freq2").with_input("freq2.flp", &flp2).depends_on(&["k2"]),
]).await?;
```

`run_sync()` is blocking (wraps a tokio runtime internally) for callers who don't want async. `ATSession` methods are async. Both handle chunk assembly and stream consumption internally.

The Rust client also serves as validation that the proto is clean from a strongly-typed language perspective. Awkwardness in the generated Rust types signals proto issues worth fixing before other languages adopt it.

### Implementation notes

**Python:** No threads anywhere. All three tiers use synchronous iteration over gRPC streams (or unary calls for Tier 1). The `on_output` / `on_step_output` callbacks are invoked inline during the `for msg in stream` loop. gRPC Python handles the HTTP/2 framing internally. Roughly 300 lines.

**Rust:** Uses tonic's async streaming. `run_sync()` wraps a tokio runtime for blocking callers. Roughly 400 lines.

## Concurrency

The server enforces serialization to prevent workspace races:

- **`Run` and `RunPipeline`** are mutually exclusive. Only one execution RPC (of either kind) may be active at a time. A second call while one is in progress returns gRPC `FAILED_PRECONDITION`.
- **`RunSync`** is always safe to call concurrently — each call uses its own temp directory. Multiple `RunSync` calls can execute in parallel, and they can overlap with `Run` or `RunPipeline` (they only read the workspace, never write to it).
- **Workspace RPCs** (`UploadFile`, `GetFile`, `DeleteFile`, `ListFiles`) can execute concurrently with each other and with `RunSync`. They are blocked while a `Run` or `RunPipeline` is active to prevent modifying files mid-execution.

This is the simplest correct policy. It matches the single-session model (one user working interactively) and avoids subtle filesystem race conditions.

## Error Handling

| Condition | Behavior |
|-----------|----------|
| Unknown model name | gRPC `INVALID_ARGUMENT` — stream never opens. |
| Invalid filename (path traversal) | gRPC `INVALID_ARGUMENT`. |
| Pipeline: cycle in dependency graph | gRPC `INVALID_ARGUMENT`. |
| Pipeline: `depends_on` references nonexistent step | gRPC `INVALID_ARGUMENT`. |
| Pipeline: duplicate step IDs | gRPC `INVALID_ARGUMENT`. |
| Concurrent `Run`/`RunPipeline` while one is active | gRPC `FAILED_PRECONDITION`. |
| Executable completes (any exit code) | `RunCompleted` with `RUN_STATUS_COMPLETED` and the exit code. The caller decides what constitutes model failure. |
| Executable times out | `RunCompleted` with `RUN_STATUS_TIMED_OUT`. Partial stdout/stderr already delivered. |
| Executable killed by signal | `RunCompleted` with `RUN_STATUS_SIGNALED`. |
| Pipeline step skipped (dependency failed) | `RunCompleted` with `RUN_STATUS_SKIPPED`. No subprocess was launched. |
| Workspace I/O error | gRPC `INTERNAL`. |

The service does **not** interpret the exit code as success/failure. AT programs use `STOP` with various messages for both normal termination and errors. The caller interprets the return code and `.prt` contents.

**Note on stdout/stderr.** AT programs open unit 6 (Fortran stdout) as the `.prt` file early in their execution. After this point, all meaningful program output goes to the `.prt` file, not to process stdout. Streamed stdout will typically be empty or contain only the Fortran runtime banner. The `.prt` file — which is returned as an output file — is where callers should look for model diagnostics. Stderr captures only Fortran runtime errors (e.g., segfaults, library warnings).

## Security

- **Allowlist enforcement**: on startup, scan `BIN_DIR` for `*.exe` files and build a `HashSet<String>`. Reject any model name not in that set.
- **Filename validation**: file names in uploads must not contain `/`, `\`, or `..`. Reject the request otherwise.
- **No shell**: use `Command::new` directly, never `sh -c`. No shell injection possible.
- **Workspace isolation**: each container has its own workspace. No cross-session access.

## Health Check

The server implements the standard gRPC health checking protocol (`grpc.health.v1.Health`). This enables:
- **Docker**: `HEALTHCHECK` via `grpc_health_probe` or a simple gRPC call.
- **Kubernetes**: readiness and liveness probes targeting the health endpoint.
- **Clients**: connection verification before submitting work.

The health status is `SERVING` once the server has started and the executable allowlist has been built. It transitions to `NOT_SERVING` during graceful shutdown.

## Logging

The binary uses **`tracing`** with a default filter of `at_runner=info,tonic=info`. Override with the standard **`RUST_LOG`** environment variable (for example `RUST_LOG=at_runner=debug` for more detail).

Logs are **structured**: each RPC (`RunSync`, `Run`, workspace calls, `RunPipeline`) emits a `session_id` (UUID) so you can correlate lines for a single request. Execution logs include model name, `file_root`, timeouts, **exit codes**, **run status** (`RunStatus`), stdout/stderr sizes (buffered path), and **output file names with sizes**; streaming paths also log chunks sent per file. This is server-side observability only — nothing is added to protobuf messages.

## Future Improvements

### Semantic API layer (at-models)

A higher-level API that sits above the client library and understands environment parameters, SSP profiles, receiver grids, and output semantics. Guided by the existing MATLAB tooling, it would let callers work with typed acoustic parameters rather than raw files. For example, constructing an `.env` file from structured Python objects, or parsing a `.shd` file into a NumPy array of complex pressure values.

### Static linking (Phase 3)

Choosing Rust keeps the door open for in-process Fortran execution:

- The `Runner` trait and protobuf interface remain unchanged.
- A second backend implementation replaces subprocess execution with FFI calls to statically-linked Fortran subroutines.
- The `--wrap` linker mechanism intercepts all file I/O at the POSIX layer, routing it through the same in-memory buffers that the current implementation uses for the workspace.
- The service crate can support both backends (subprocess vs. in-process) via a feature flag or runtime configuration.

This is a significant engineering effort (estimated 4-8 weeks) and is not needed until subprocess + tmpfs overhead becomes a measured bottleneck.

### Multi-session gateway

A thin gateway that creates containers on demand, routes clients, and destroys containers after idle timeout. In Kubernetes, each session becomes a Pod managed by a controller. The at-runner service itself would not change — the orchestration is purely external. Cross-session file sharing (e.g. a shared bathymetry library) is explicitly out of scope and would require shared storage with consistency guarantees.
