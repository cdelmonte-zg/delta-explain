# Install

Pick whichever fits your environment. All routes ship the same binary; the
Python wheel bundles it plus a thin API.

## Homebrew (macOS, Linux)

```bash
brew tap cdelmonte-zg/tap
brew install delta-explain
```

## Scoop (Windows)

```powershell
scoop bucket add cdelmonte-zg https://github.com/cdelmonte-zg/scoop-bucket
scoop install delta-explain
```

## PyPI (no Rust needed)

```bash
pip install delta-explain
```

The wheel ships the compiled binary (the `delta-explain` command works from the
same environment) plus a thin Python API — see [From Python](../guides/python.md).

## Cargo (crates.io)

```bash
cargo install delta-explain
```

## Docker (amd64 + arm64)

```bash
docker run --rm -v /path/to/table:/data ghcr.io/cdelmonte-zg/delta-explain \
  /data -w "col > 10"
```

For pipelines, pin a release tag or a digest; `:latest` is for local
exploration only.

## Pre-built binaries and `.deb`

Every release attaches archives for six targets (Linux glibc/musl/ARM64, macOS
Intel/Apple Silicon, Windows) each with a `.sha256`, plus `.deb` packages for
amd64 and arm64. Grab them from the
[latest release](https://github.com/cdelmonte-zg/delta-explain/releases/latest).
The musl build is statically linked and runs on any Linux distribution.
