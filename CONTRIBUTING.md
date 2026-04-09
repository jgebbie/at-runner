# Contributing to AT Runner

Thank you for contributing. This document describes how we work, how to propose changes, and how releases and versioning are managed.

## Code of conduct

Be respectful, constructive, and professional. We follow the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/) (version 2.1). If you see behavior that violates it, please report it to the maintainers.

## Security issues

Do **not** open a public issue for security vulnerabilities. See **[SECURITY.md](SECURITY.md)** for how to report them (GitHub Security Advisories and maintainer contact).

## Getting started

1. **Fork** the repository and clone your fork.
2. Create a **branch** from `main` (see [Branches](#branches)).
3. Make changes with **clear commits** (see [Conventional commits](#conventional-commits)).
4. Open a **pull request** against `main` and fill out the PR template.

### Prerequisites

- **Rust** (stable) and **protobuf compiler** (`protoc`) for the Cargo workspace at the repo root (`service/`, `client/rust/`, `testing/rust/`).
- **Python 3.12+** for the Python client and tooling (see [README.md](README.md)).
- **Docker** (optional) for the full image and integration-style flows.

## Branches

- **`main`** — release-ready history; protected in CI.
- **Feature branches** — use short, descriptive names, for example `feat/grpc-timeout`, `fix/workspace-race`, `docs/readme-typo`.

Avoid long-lived branches; prefer small, reviewable PRs.

## Git hooks (prek)

We use **[prek](https://github.com/j178/prek)** — a Rust implementation that runs the same hook configuration as [pre-commit](https://pre-commit.com/) (this repo’s [`.pre-commit-config.yaml`](.pre-commit-config.yaml)). Hooks cover common file checks and **`cargo fmt`** for the whole Rust workspace (repo root).

1. Install **prek** (see the [prek repository](https://github.com/j178/prek) for installers, e.g. `cargo install prek`, Homebrew, or the release binary).
2. From the repository root: **`prek install`** — registers the Git hook.
3. Before pushing, run **`prek run --all-files`** (or rely on the hook on `git commit`).

CI runs the same configuration via [`j178/prek-action`](https://github.com/j178/prek-action) (see [GitHub workflows](#github-workflows)).

## Conventional commits

All commits on the default branch (and ideally every commit on a PR branch) should follow **[Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/)**:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

Common **types** (lowercase):

| Type | Use |
|------|-----|
| `feat` | New user-visible behavior or API |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no logic change |
| `refactor` | Internal change without behavior change |
| `perf` | Performance improvement |
| `test` | Tests only |
| `build` | Build system, CI, dependencies |
| `ci` | CI configuration |
| `chore` | Maintenance (e.g. tooling) |

**Breaking changes** must be indicated in the footer:

```
feat(api): rename RunSync field

BREAKING CHANGE: RunSyncResponse field `output` is now `files`.
```

Or use `feat!:` / `fix!:` in the subject line.

**Scopes** are optional. Examples: `feat(service):`, `fix(client/python):`, `docs(ci):`.

### Interactive commits (recommended)

Install [Commitizen](https://commitizen-tools.github.io/commitizen/) (Python) in a virtual environment:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[dev]"
```

Create commits interactively:

```bash
cz commit
```

This runs a prompt that matches the Conventional Commits format.

### Validating commits locally

Check that a range of commits follows the rules:

```bash
cz check --rev-range main..HEAD
```

## Versioning and releases (Commitizen)

We use **Semantic Versioning** (`MAJOR.MINOR.PATCH`) and **[Commitizen](https://commitizen-tools.github.io/commitizen/)** to bump versions and maintain the changelog.

- **Configuration** lives in the root [`pyproject.toml`](pyproject.toml) under `[tool.commitizen]`.
- **Versioned files** include `service/Cargo.toml`, `client/python/pyproject.toml`, `client/rust/Cargo.toml`, and the workspace `pyproject.toml` (kept in sync). Rust uses a single **`Cargo.lock`** at the repository root; after bumping crate versions, run `cargo build --workspace` (or any build from the repo root) so the lockfile stays aligned.

### Bump

You need a **git repository with at least one commit** (Commitizen reads history and tags). After conventional commits are on `main` (or on a release branch), a maintainer runs:

```bash
cz bump
```

This:

1. Chooses the next version from commit history (including `BREAKING CHANGE` / `feat!` / `fix!`).
2. Updates `CHANGELOG.md` (Keep a Changelog style).
3. Updates the version fields listed in `version_files`.
4. Creates a commit and (by default) a tag `vX.Y.Z`.

Use `cz bump --dry-run` to preview. Options such as `--increment PATCH|MINOR|MAJOR` are available when you need a manual override.

**Tags** follow `v$version` (for example `v0.2.0`).

## Pull requests

- **One logical change** per PR when possible.
- **Describe the “why”** in the PR body.
- **Link issues** with `Fixes #123` or `Refs #123` in the footer of a commit or in the PR description.
- **Keep CI green** (see [GitHub workflows](#github-workflows)).
- **Squash merge** is preferred for a tidy history; the final squashed commit message should remain a valid Conventional Commit.

## GitHub workflows

Automation lives under [`.github/workflows/`](.github/workflows/).

| Workflow | Purpose |
|----------|---------|
| **CI** | **Prek** hooks; Rust workspace `clippy` / `build` / `test` (server + test driver; client crate tests compiled with `--no-run` because integration tests need a running server); Python client install + byte-compile — on pushes and PRs to **`main`**. |
| **Commit messages** | On pull requests, validates that commits in the PR range follow Conventional Commits (`cz check`). |

Forks receive the same checks on PRs.

## Style notes

- **Rust:** `cargo fmt` (via **prek** locally and in CI) and `cargo clippy` (see CI). Match existing patterns; run Cargo commands from the repo root so the workspace lockfile applies.
- **Python:** Follow the style of `client/python/`; keep public API changes documented in the PR.
- **Protobuf:** API changes belong in `proto/` and require a version bump and changelog entry under the appropriate `feat`/`fix`/`break` rules.

## Questions

Open a [discussion](https://docs.github.com/en/discussions) (if enabled) or an issue with the `question` label. Maintainers will help triage.
