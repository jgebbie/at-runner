# AT Runner

**AT Runner** is a gRPC service that wraps the [Acoustics Toolbox (AT)](https://oalib-acoustics.org/) — Fortran programs such as BELLHOP, KRAKEN, SCOOTER, and SPARC used for underwater acoustic modeling. Clients send input files, run models over the network, and retrieve outputs without managing Fortran builds on their machines.

This repository holds the **runner only** (Rust server, Protocol Buffers, and client libraries). The Fortran sources and canonical regression tests live in the AT repository (e.g. [github.com/jgebbie/at](https://github.com/jgebbie/at)). The integration boundary is intentional: ship AT as prebuilt binaries (OCI image or a local `bin/` directory), pin a version for reproducible images, and avoid vendoring the full Fortran tree here long term.

For **API design, session model, supported executables, streaming behavior, and client patterns**, see **[ARCHITECTURE.md](ARCHITECTURE.md)**.

## Layout

```
├── Cargo.toml          # Rust workspace (service, client/rust, testing/rust); Cargo.lock at repo root
├── proto/              # at.runner.v1 — shared by server and clients
├── service/            # Rust gRPC server (at-runner binary)
├── client/
│   ├── python/         # oalib-at-runner distribution, at_runner import package
│   └── rust/           # Rust client crate
├── scripts/            # Server helpers, smoke/integration/sweep tests
├── testing/            # Docker Compose + Python/Rust test drivers
├── external/           # (optional, gitignored) AT clone from fetch-at-tests.sh → external/at/
└── Dockerfile          # Multi-stage: AT binaries + Rust build → runtime image
```

Fortran **test fixtures** are not committed here. Use `./scripts/fetch-at-tests.sh` to clone AT into `external/at/` (under `external/`, gitignored), or set `AT_TESTS_ROOT` to any checkout’s `tests/` directory.

## Requirements

- **Rust** (workspace in root `Cargo.toml`; edition `2021` in member crates) and **protobuf compiler** (`protoc`) to build from source.
- **Docker** (optional) for the full image and scripted tests.
- **Python 3.12+** for the client and shell test scripts, for example:
  `python3 -m venv client/python/.venv && . client/python/.venv/bin/activate && pip install -e './client/python[dev]'`
  (run from the repository root; `[dev]` pulls in pytest and codegen tools).
- **[prek](https://github.com/j178/prek)** (Rust; compatible with `.pre-commit-config.yaml`) for git hooks — install the binary, then run `prek install` at the repo root. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Build and run

### Docker (default / recommended)

The [Dockerfile](Dockerfile) takes an **`AT_IMAGE`** build arg for the first stage (AT binaries). The default is pinned to **`ghcr.io/jgebbie/at:at_2026_2_2`** because the `:latest` tag may not carry every platform manifest.

**Recommended** (pinned AT, matches what the [release workflow](.github/workflows/release.yml) uses when publishing to GHCR):

```bash
docker build -t at-runner .
docker run -p 50051:50051 \
  --tmpfs /workspace:rw,noexec,nosuid,size=512m \
  at-runner
```

The published runner image is intended to be public and pullable without `docker login`.
If GHCR returns `unauthorized`, the GitHub package visibility is not public yet;
set `ghcr.io/jgebbie/at-runner` to **Public** in the package settings and rerun
the release workflow. Public source builds that use the default `AT_IMAGE` also
need the `ghcr.io/jgebbie/at` package to be public; otherwise override
`AT_IMAGE` with a readable image or local artifact.

Helper (default Docker): `./scripts/server-start.sh` and `./scripts/server-stop.sh`. The helper also defaults to the pinned GHCR image. Override `AT_IMAGE` to use another published tag or a local artifact image:

```bash
AT_IMAGE=at-binaries-local ./scripts/server-start.sh
```

### Local Rust binary

The repo root is a **Cargo workspace** (see root `Cargo.toml`); the server binary is built with `-p at-runner`.

```bash
cargo build --release -p at-runner
# Run against AT executables on disk (same layout as make install → bin/)
./target/release/at-runner --bin-dir /path/to/at/bin --workspace /tmp/at-ws --port 50051
```

The local `./scripts/server-start.sh --local` path expects **`bin/`** at the repository root (populate it from an AT `make install` or symlink).

## Tests

Integration tests use real case files from the AT repo’s `tests/` tree.

```bash
./scripts/fetch-at-tests.sh          # once: clones github.com/jgebbie/at → external/at/
./scripts/test-smoke.sh              # quick three-tier API check (needs running server)
./scripts/test-integration.sh        # pytest (needs running server + venv)
./scripts/test-sweep.sh              # broad RunSync sweep (needs running server + venv)
./scripts/test-sweep-compose.sh      # sweep via a docker-compose runner pool (no local venv)
```

Set **`AT_RUNNER_TARGET`** (default `localhost:50051`) if the server is not local.

`test-sweep.sh` can also distribute work across a **runner pool** when `RUNNERS` is set:

```bash
RUNNERS="runner-1:50051,runner-2:50051,runner-3:50051,runner-4:50051,runner-5:50051,runner-6:50051" ./scripts/test-sweep.sh
```

**Compose harness** (multiple runners + drivers): build the **`at-runner`** image once from the repository root (same tag the Compose file expects). Prefer pinning `AT_IMAGE` for consistency with releases, for example:

```bash
docker build --build-arg AT_IMAGE=ghcr.io/jgebbie/at:at_2026_2_2 -t at-runner .
```

Then after `fetch-at-tests.sh` (or set `AT_TESTS_COMPOSE_MOUNT` to your `tests/` path), from `testing/`:

```bash
docker compose -f docker-compose.yml up --abort-on-container-exit
```

To run just the **sweep** against the compose runner pool:

```bash
./scripts/test-sweep-compose.sh
```

The script runs the sweep in a one-off container (`docker compose run --rm … sweep-driver`) so the sweep-driver container is removed when the sweep exits, and it runs `docker compose down` when the sweep finishes or is interrupted so runner containers and the project network are stopped and removed.

If you see widespread `SIGILL` / “Illegal instruction” failures from AT executables on older CPUs, run the compose sweep with a locally-built, portable AT binaries image:

```bash
BUILD_AT_LOCAL=1 ./scripts/test-sweep-compose.sh
```

## Python client

The Python client is published to PyPI as **`oalib-at-runner`** and imported as
`at_runner`:

```bash
python -m pip install oalib-at-runner
```

For local development from this checkout, keep using the editable install:

```bash
python3 -m venv client/python/.venv
. client/python/.venv/bin/activate
pip install -e './client/python[dev]'
```

## Conventions (summary)

- **File payloads** on the wire are raw **bytes** (Fortran emits binary `.mod`, `.shd`, etc.).
- Every model takes a **file root** as its first CLI argument; filenames are that root plus extensions (e.g. `MunkK.env`).
- **One container ≈ one session**: workspace under `/workspace` (use tmpfs in production); `Run` / `RunPipeline` are serialized against the workspace; stateless `RunSync` can run concurrently.
- **Server logs**: `RUST_LOG` controls verbosity; each RPC gets a `session_id` in log output for correlation (see **Logging** in [ARCHITECTURE.md](ARCHITECTURE.md)).

Details: [ARCHITECTURE.md](ARCHITECTURE.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the code of conduct, [Conventional Commits](https://www.conventionalcommits.org/), and what runs in GitHub Actions. For releases, **[RELEASE.md](RELEASE.md)** describes `cz bump`, lockfile refresh, and pushing tags.

## Related repositories

| Repository | Role |
|------------|------|
| AT (Fortran) | Sources, `make install` → `bin/`, `tests/` regression cases |
| AT Runner (this repo) | gRPC service, proto, clients, integration Dockerfile |

Typical linking options: **prebuilt AT base image** (as in this Dockerfile), **git submodule**, or **BuildKit additional context** pointing at an AT checkout — pick one and pin a tag or digest for reproducible builds.

## License

AT Runner is released under the **GNU General Public License v3** only; see
[LICENSE](LICENSE). This keeps runner distribution aligned with the GPLv3
Acoustics Toolbox binaries that the service executes in combined images.
