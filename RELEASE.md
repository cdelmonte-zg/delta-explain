# Releasing delta-explain

The release itself is automated (pushing a `v*` tag triggers
`.github/workflows/release.yml`: crates.io publish, binaries for six
targets, `.deb` packages, Homebrew tap and Scoop bucket bumps; `docker.yml`
builds and pushes the image). This checklist is everything around that
automation: what to verify before the tag, and what the automation does
not verify for you afterwards. `scripts/release.sh` drives the manual
part (`status`, `prepare`, `tag`, `verify`); the items below say which
step covers what, and what stays by hand.

## 1. Pre-flight, on a release branch

- [ ] `scripts/release.sh prepare X.Y.Z` (from a clean, up-to-date main):
      creates `release/X.Y.Z`, bumps `Cargo.toml` and `Cargo.lock`, dates
      the `[Unreleased]` section of CHANGELOG.md, pins
      `cdelmonte-zg/delta-explain@vX.Y.Z` in README and the CI guide, runs
      `cargo fmt --check`, `clippy`, `test`, `cargo publish --dry-run`, the
      release build and the maturin wheel with a clean-venv install, then
      commits `chore: release as X.Y.Z` and opens the PR (`--dry-run`
      stops before the push, `--skip-checks` leaves the checks to CI).
- [ ] Differential harness, fresh mode (the survivor-set oracle):

  ```bash
  cd examples/differential && docker compose up -d && cd -
  DX_BIN=$PWD/target/release/delta-explain DX_DIFF_FRESH=1 \
      python3 examples/differential/run_differential.py   # expect SOUND on all predicates
  ```

- [ ] **Review README.md and docs/.** crates.io freezes the README at
      publish time; whatever is wrong in it stays wrong for this version
      forever. Check in particular that performance numbers are not stale
      and that every example still matches the output of the bumped build.
- [ ] **CHANGELOG.md**: read the section `prepare` just dated. If the JSON
      output changed, state the `schema_version` implication explicitly
      (additive = minor, breaking = major) and confirm
      `render::SCHEMA_VERSION`, `schemas/report-vX.Y.schema.json`, and the
      docs agree. The section becomes the release notes: `release.yml`
      prepends it to the generated ones.
- [ ] If the release changed the CLI output or the report shape,
      regenerate the committed artifacts that carry captured output and
      the tool version, all against the bumped build so the banners match
      the released version: `python3 scripts/render_readme_shots.py` for
      the README terminal shots (`assets/readme/cli-*.png`; the catalog
      shot reads `sales.events.orders` from the `examples/catalogs` stack,
      prepared by `scripts/register_shot_table.sh`), then re-execute
      `examples/taxi-optimization` (its README run instructions) and
      refresh `viewer/screenshot.png` by hand. Commit them on the release
      branch.
- [ ] Wait for full CI (including `docker-smoke`), merge.

## 2. Tag

- [ ] `scripts/release.sh tag X.Y.Z`: checks that main carries the bump
      and the dated changelog, tags it, pushes the tag, watches
      `release.yml` and `docker.yml` to completion and runs `verify`
      (`--no-watch` returns right after the push, `--dry-run` stops before
      the tag).

## 3. Post-release verification (the automation fails silently here)

- [ ] `scripts/release.sh verify X.Y.Z` (also run by `tag`) checks: the
      GitHub release with all assets (six target archives with `sha256`
      sums, `.deb` amd64+arm64); the Homebrew formula and the Scoop
      manifest at the new version *with the published checksums*, not
      placeholders; crates.io and PyPI; the ghcr image with both amd64 and
      arm64 manifests; the linux x86_64 tarball (checksum, `--version`,
      a run on the fixture) and `pip install delta-explain==X.Y.Z` in a
      clean venv. Re-run it if PyPI was still propagating.
- [ ] External-user check, from a machine or directory with no repo
      checkout: download the *latest* shortcut URL, verify `sha256sum`,
      run `delta-explain --version`.
- [ ] `cargo install delta-explain --version X.Y.Z` succeeds and
      `--version` matches.
- [ ] Docker: `docker run --rm ghcr.io/cdelmonte-zg/delta-explain:X.Y.Z --version`.
- [ ] GitHub Action smoke: a workflow using
      `cdelmonte-zg/delta-explain@vX.Y.Z` with `version: X.Y.Z` passes.
- [ ] **Marketplace listing points at the new tag.** Releases created by
      CI are published to the GitHub Marketplace automatically (verified
      on v0.7.0: the "Publish this Action" checkbox arrives already
      ticked and the listing follows the latest release) - so this is a
      verification, not an action. Check that the listing advertises the
      released tag, from the UI at
      https://github.com/marketplace/actions/delta-explain or from a
      shell:

  ```bash
  curl -sL https://github.com/marketplace/actions/delta-explain \
      | grep -o '"latestRelease":{"tagName":"[^"]*"'   # expect vX.Y.Z
  ```

  If it does not, edit the release in the UI and tick "Publish this
      Action to the GitHub Marketplace". Note the `action.yml`
      description must stay under 125 characters or the publication is
      rejected.
- [ ] **homebrew-tap and scoop-bucket really got bumped**: the bots update
      them but fail silently (placeholder checksums have happened before).
      `verify` compares both against the published `sha256` assets; if it
      warned, fix the formula or manifest by hand.
- [ ] **Documentation site deployed and current.** Cutting the CHANGELOG
      to the new version is a docs change, so `pages.yml` redeploys the
      mdBook site on the merge to main. Open
      <https://cdelmonte-zg.github.io/delta-explain/> and confirm: landing
      and search work, the reference pages (symlinked `semantics` /
      `json-schema` / `validation`) render, the JSON-schema link resolves,
      and the Changelog/Roadmap pages show the release just cut.
      One-time-only, before the first deploy: enable Pages in
      **Settings -> Pages -> Source: GitHub Actions** (the workflow cannot
      enable it, and without it the site never goes live).

## 4. Aftermath

- [ ] The README example and docs referencing a pinned tag point at the
      version just released (`prepare` did it; confirm nothing was added
      since).
- [ ] If the release narrative matters (webinar, article), write it from
      the CHANGELOG section, not the other way around.
