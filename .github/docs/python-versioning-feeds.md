# Python Versioning & Feed Publishing

This document describes how Python package versions are calculated and where they are published across CI/CD and Release workflows.

## Packages

This monorepo produces five Python packages:

| Package | Directory |
|---------|-----------|
| `snowpark-checkpoints` | (root) |
| `snowpark-checkpoints-configuration` | `snowpark-checkpoints-configuration/` |
| `snowpark-checkpoints-hypothesis` | `snowpark-checkpoints-hypothesis/` |
| `snowpark-checkpoints-validators` | `snowpark-checkpoints-validators/` |
| `snowpark-checkpoints-collectors` | `snowpark-checkpoints-collectors/` |

## Versioning

Versions are derived from the latest git tag. Unlike other repos in this org, this repo does **not** use GitVersion — it reads the version directly from `git describe --tags`.

### Version by Event

| Event | Version | Example |
|-------|---------|---------|
| **Release** (tag) | `X.Y.Z` | `1.2.0` |
| **CI/CD** (push to main / PR) | `X.Y.Z` (from latest tag) | `1.2.0` |

> Note: CI/CD builds use the same version from the latest tag. When published to Azure DevOps or TestPyPI, duplicates are handled gracefully via `--skip-existing` / 409 Conflict handling.

### PEP 440 Compliance

All versions follow [PEP 440](https://peps.python.org/pep-0440/):

```
1.2.0rc1     ← release candidate
1.2.0        ← STABLE RELEASE
```

### Key Behaviors

- `pip install package` → installs only **stable** releases (e.g., `1.2.0`)
- `pip install --pre package` → installs the latest pre-release (e.g., `1.2.0rc1`)

## Publishing Feeds

### Feed Matrix

| Workflow | Azure DevOps | TestPyPI | PyPI |
|----------|-------------|----------|------|
| **CI/CD** (push to main / PR) | ✅ | ✅ | — |
| **Release** (tag) | ✅ | — | ✅ |

### Feed URLs

| Feed | URL | Auth |
|------|-----|------|
| Azure DevOps | `https://pkgs.dev.azure.com/snowmountain/_packaging/snowmountain-python/pypi/upload/` | `ADO_FEED_PAT` |
| TestPyPI | `https://test.pypi.org/legacy/` | `PYPI_TEST_API_TOKEN` |
| PyPI | `https://upload.pypi.org/legacy/` | `PYPI_API_TOKEN` |

### Installing from Azure DevOps

```bash
pip install snowpark-checkpoints \
  --extra-index-url https://${ADO_PAT}@pkgs.dev.azure.com/snowmountain/_packaging/snowmountain-python/pypi/simple/
```

### Installing from TestPyPI

```bash
pip install snowpark-checkpoints \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/
```

## Workflows

| Workflow | Trigger | Publishes To |
|----------|---------|-------------|
| `snowpark-checkpoints-publish.yml` | push to main, PR | Azure DevOps, TestPyPI |
| `snowpark-checkpoints-publish-external.yml` | release published | Azure DevOps, PyPI |
