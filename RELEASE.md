# Release checklist

This repository uses **[Commitizen](https://commitizen-tools.github.io/commitizen/)** for version bumps and changelog updates. Configuration lives in the root [`pyproject.toml`](pyproject.toml) under `[tool.commitizen]`.

## What a fresh clone needs

No extra hook installation beyond:

1. **Python tooling** — editable install with dev deps so `cz` is available:
   `pip install -e ".[dev]"`
2. **Rust toolchain** — for the pre-bump hook that refreshes [`Cargo.lock`](Cargo.lock) (`cargo build --workspace`).

The **`pre_bump_hooks`** entry in `pyproject.toml` is part of the repo; Commitizen reads it automatically when you run `cz bump`. You do not configure hooks in Git separately for this behavior.

## Why `pre_bump_hooks` (not `post_bump_hooks`)

Commitizen runs **`post_bump_hooks` after the bump commit and tag**. The root **`Cargo.lock`** must be updated **before** that commit so the lockfile is included in the same commit as the version bumps. The project therefore uses **`pre_bump_hooks`**, which run after version fields are written but **before** `git commit`.

## Maintainer steps

1. Ensure **`main`** (or your release branch) has the conventional commits you want in this release.
2. Install tooling if needed: `pip install -e ".[dev]"`, Rust stable, `protoc`.
3. Preview (optional): `cz bump --dry-run`
4. Run: **`cz bump`** (or `cz bump --increment PATCH|MINOR|MAJOR` for an explicit bump).
5. Commitizen will:
   - update versions in the files listed in **`version_files`** (including `testing/rust/Cargo.toml`);
   - update **`CHANGELOG.md`**;
   - run **`scripts/commitizen-pre-bump-cargo-lock.sh`** to refresh **`Cargo.lock`**;
   - create the bump **commit** and **`vX.Y.Z`** tag.
6. Review the commit diff (especially `Cargo.lock` and changelog).
7. Push the branch and the tag: `git push origin main` and `git push origin vX.Y.Z` (adjust branch/tag names as appropriate).

Pushing the **`v*`** tag triggers the [release workflow](.github/workflows/release.yml):

- publish the runner image to GHCR;
- verify the GHCR image is anonymously readable;
- build, check, and publish the Python client distribution
  **`oalib-at-runner`** to PyPI.

Before the first PyPI release, configure PyPI Trusted Publishing for:

- Owner: `jgebbie`
- Repository: `at-runner`
- Workflow: `release.yml`
- Environment: `pypi`

Because `v0.3.0` predates Python publishing in this workflow, a maintainer can
backfill that first PyPI release with `workflow_dispatch` after the trusted
publisher and GHCR visibility are configured. Use the existing image tag and set
`publish_python` to true.

Before relying on public container pulls, set the GitHub package
`ghcr.io/jgebbie/at-runner` to **Public** in the package settings. Public GHCR
container packages are anonymously pullable; the release workflow verifies this
after publishing. If public users should also build this Dockerfile with the
default `AT_IMAGE`, set `ghcr.io/jgebbie/at` to **Public** as well.

## If the pre-bump hook fails

The hook runs `cargo build --workspace` from the repository root. Fix the reported Cargo error, then either:

- run `cz bump` again if the bump did not complete, or
- refresh the lockfile manually (`cargo build --workspace`), commit, and align versions by hand if you are recovering from a partial run.

See also **[CONTRIBUTING.md](CONTRIBUTING.md)** (versioning section) for `version_files` and conventional commits.
