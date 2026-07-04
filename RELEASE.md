# Releasing delta-explain

The release itself is automated (pushing a `v*` tag triggers
`.github/workflows/release.yml`: crates.io publish, binaries for six
targets, `.deb` packages, Homebrew tap and Scoop bucket bumps; `docker.yml`
builds and pushes the image). This checklist is everything around that
automation: what to verify before the tag, and what the automation does
not verify for you afterwards.

## 1. Pre-flight, on a release branch

- [ ] `cargo fmt --check`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] Differential harness, fresh mode (the survivor-set oracle):

  ```bash
  cd examples/differential && docker compose up -d && cd -
  DX_BIN=$PWD/target/release/delta-explain DX_DIFF_FRESH=1 \
      python3 examples/differential/run_differential.py   # expect SOUND on all predicates
  ```

- [ ] **Review README.md and docs/.** crates.io freezes the README at
      publish time; whatever is wrong in it stays wrong for this version
      forever. Check in particular: the action example pins the tag being
      released (`cdelmonte-zg/delta-explain@vX.Y.Z`, not `@main`), the
      *Current limitations* section reflects reality, performance numbers
      are not stale.
- [ ] **CHANGELOG.md**: move `[Unreleased]` under the new version heading
      with the date. If the JSON output changed, state the
      `schema_version` implication explicitly (additive = minor, breaking
      = major) and confirm `render::SCHEMA_VERSION`,
      `schemas/report-vX.Y.schema.json`, and the docs agree.
- [ ] Bump `version` in `Cargo.toml`; `cargo build` to refresh
      `Cargo.lock`; commit as `chore: release as X.Y.Z`.
- [ ] `cargo publish --dry-run`
- [ ] `maturin build --release` locally: the wheel builds and installs in a
      clean venv, `import delta_explain` and the bundled binary both work
- [ ] Open the PR, wait for full CI (including `docker-smoke`), merge.

## 2. Tag

- [ ] Tag the merge commit on main and push it:

  ```bash
  git tag vX.Y.Z <merge-commit> && git push origin vX.Y.Z
  ```

- [ ] Watch the release and docker workflows to completion.

## 3. Post-release verification (the automation fails silently here)

- [ ] GitHub release exists with all assets: six target archives, `.deb`
      amd64+arm64, `sha256` sums.
- [ ] External-user check, from a machine or directory with no repo
      checkout: download the *pinned* asset URL and the *latest* shortcut
      URL, verify `sha256sum`, run `delta-explain --version`.
- [ ] `cargo install delta-explain --version X.Y.Z` succeeds and
      `--version` matches.
- [ ] `pip install delta-explain==X.Y.Z` in a clean venv: `delta-explain
      --version` matches and `python -c "import delta_explain"` works
      (first release only: the PyPI trusted publisher must be configured
      before the tag, see the repository settings note in the PR that
      introduced the wheels).
- [ ] Docker image: `docker run --rm ghcr.io/cdelmonte-zg/delta-explain:X.Y.Z --version`
      (both amd64 and arm64 manifests present).
- [ ] GitHub Action smoke: a workflow using
      `cdelmonte-zg/delta-explain@vX.Y.Z` with `version: X.Y.Z` passes.
- [ ] **Marketplace republication (manual, UI-only)**: releases created by
      CI are not published to the GitHub Marketplace automatically. Edit
      the release in the UI, tick "Publish this Action to the GitHub
      Marketplace", update. Verify the listing points at the new tag:
      https://github.com/marketplace/actions/delta-explain. Note the
      `action.yml` description must stay under 125 characters or the
      publication is rejected.
- [ ] **homebrew-tap and scoop-bucket really got bumped**: the bots update
      them but fail silently. Open both repos and check the formula and
      manifest carry the new version *and real checksums*, not
      placeholders (this has happened before).

## 4. Aftermath

- [ ] The README example and docs referencing a pinned tag point at the
      version just released.
- [ ] If the release narrative matters (webinar, article), write it from
      the CHANGELOG section, not the other way around.
