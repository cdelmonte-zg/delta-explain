# Packaging

Distribution channels other than crates.io and the GHCR Docker image. Each channel needs a one-time setup; afterwards the release workflow handles updates automatically.

## Channels

| Channel | Manifest | Auto-update | One-time setup |
|---|---|---|---|
| Debian/Ubuntu `.deb` | `Cargo.toml` `[package.metadata.deb]` | release workflow `build-deb` job | none (already wired) |
| Homebrew tap | `homebrew/delta-explain.rb` | release workflow `update-homebrew-tap` job | create tap repo + PAT secret |
| Scoop bucket | `scoop/delta-explain.json` | bucket repo's own `checkver`/`autoupdate` workflow | create bucket repo |

## Homebrew tap setup (one-time)

1. Create a public GitHub repo named exactly `homebrew-tap` under your account: `cdelmonte-zg/homebrew-tap`. The `homebrew-` prefix is required by Homebrew so users can `brew tap cdelmonte-zg/tap`.
2. Copy `homebrew/delta-explain.rb` into the tap repo at `Formula/delta-explain.rb`. After the first `v*` release publishes the binaries, replace the four `REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE` placeholders with the real SHA256 of each tarball.
3. In `cdelmonte-zg/delta-explain`, create a repository secret named `HOMEBREW_TAP_TOKEN` containing a Personal Access Token (classic) with `repo` scope. The release workflow uses it to push updated formulas to the tap.
4. From the second release onwards, the `update-homebrew-tap` job in `release.yml` regenerates `Formula/delta-explain.rb` automatically with fresh SHA256s and commits to the tap.

End-user install:

```bash
brew tap cdelmonte-zg/tap
brew install delta-explain
```

## Scoop bucket setup (one-time)

1. Create a public GitHub repo named `scoop-bucket` under your account: `cdelmonte-zg/scoop-bucket`. The repo name is conventional (Scoop accepts any name, but `scoop-bucket` is the de facto standard).
2. Copy `scoop/delta-explain.json` into the bucket repo at `bucket/delta-explain.json`. After the first release, replace the `REPLACE_WITH_SHA256_AFTER_FIRST_RELEASE` placeholder with the SHA256 of `delta-explain-x86_64-pc-windows-msvc.zip`.
3. Add Scoop's auto-update workflow to the bucket repo (the canonical one is shipped at `https://github.com/ScoopInstaller/GithubActions`). Once configured, the workflow runs `scoop checkver -u` periodically against the manifest's `checkver`/`autoupdate` blocks and commits the new version automatically.

End-user install:

```powershell
scoop bucket add cdelmonte-zg https://github.com/cdelmonte-zg/scoop-bucket
scoop install delta-explain
```

## `.deb` (already wired)

The `build-deb` job in `release.yml` builds `delta-explain_<version>_amd64.deb` and `delta-explain_<version>_arm64.deb` on every `v*` tag and uploads them as Release assets. Users install via:

```bash
wget https://github.com/cdelmonte-zg/delta-explain/releases/download/v0.2.3/delta-explain_0.2.3-1_amd64.deb
sudo dpkg -i delta-explain_0.2.3-1_amd64.deb
```

This is `dpkg`-managed (uninstall via `sudo apt remove delta-explain`) but not served from an apt repository. Hosting a real apt repo (e.g., on Cloudsmith) is a separate, later step.

## First release: validation checklist

Before tagging the first `v*` release after this setup:

- [ ] `LICENSE` file present in repo root (used by `cargo-deb`).
- [ ] `Cargo.toml` `[package.metadata.deb]` block present.
- [ ] `homebrew-tap` repo created, initial formula committed (with placeholder SHAs).
- [ ] `scoop-bucket` repo created, initial manifest committed (with placeholder SHA).
- [ ] `HOMEBREW_TAP_TOKEN` secret added to `cdelmonte-zg/delta-explain` settings.

Then tag a `vX.Y.Z-test` release first to validate the full pipeline (6 binary targets + 2 deb files + tap update). Once green, tag the real `vX.Y.Z`.
