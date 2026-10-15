#!/usr/bin/env bash
# Drive a delta-explain release from a local checkout.
#
# The pipeline itself lives in .github/workflows/release.yml (tag `vX.Y.Z`:
# crates.io publish, six target archives plus .deb packages on the GitHub
# release, wheels to PyPI, Homebrew tap and Scoop bucket bumps) and
# docker.yml (the ghcr.io image). This script covers the manual part
# around it, the pre-flight of RELEASE.md, and checks the result:
#
#   scripts/release.sh status            what main looks like since the last tag
#   scripts/release.sh prepare X.Y.Z     on a release/X.Y.Z branch: bump
#                                        Cargo.toml/Cargo.lock, date the
#                                        changelog, pin the action tag in the
#                                        docs, run the local checks, open the PR
#   scripts/release.sh tag X.Y.Z         after the PR is merged: tag main, push
#                                        the tag, watch both workflows, verify
#   scripts/release.sh verify X.Y.Z      check the release assets, the tap
#                                        formula and Scoop manifest (real
#                                        checksums), crates.io, PyPI, the ghcr
#                                        manifests, and smoke test the linux
#                                        tarball and the wheel
#
# Options:
#   --dry-run     prepare: bump, check and commit locally, but do not push
#                 or open the PR. tag: stop before creating the tag.
#   --skip-checks prepare: skip the cargo/maturin checks (CI runs them on
#                 the PR anyway).
#   --no-watch    tag: push the tag and return without waiting for the
#                 workflows (run `verify` by hand later).
#
# What prepare does not do, because it needs a human or a running stack:
# the differential harness in fresh mode, the README terminal shots
# (scripts/render_readme_shots.py against the bumped build), the taxi
# notebook and viewer screenshot, the README read-through. It lists them
# at the end; they land as further commits on the release branch.

set -euo pipefail

REPO="cdelmonte-zg/delta-explain"
TAP_REPO="cdelmonte-zg/homebrew-tap"
SCOOP_REPO="cdelmonte-zg/scoop-bucket"
CRATE="delta-explain"
IMAGE="ghcr.io/cdelmonte-zg/delta-explain"
TARGETS=(
  x86_64-unknown-linux-gnu
  x86_64-unknown-linux-musl
  aarch64-unknown-linux-gnu
  x86_64-apple-darwin
  aarch64-apple-darwin
  x86_64-pc-windows-msvc
)
TAP_TARGETS=(aarch64-apple-darwin x86_64-apple-darwin aarch64-unknown-linux-gnu x86_64-unknown-linux-gnu)
PINNED_DOCS=(README.md book/src/guides/ci-gating.md)

usage() {
  sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-0}"
}

say()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()   { printf '\033[1;32m ok \033[0m %s\n' "$*"; }
warn() { printf '\033[1;33mwarn\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------- helpers

need_tools() {
  local t
  for t in git gh cargo curl jq python3; do
    command -v "$t" >/dev/null 2>&1 || die "missing tool: $t"
  done
}

check_version_syntax() {
  [[ "$1" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]] \
    || die "version must look like 1.2.3 or 1.2.3-rc1 (no leading v): got '$1'"
}

is_prerelease() { [[ "$1" == *-* ]]; }

cargo_version() {
  local n
  n=$(grep -c '^version = "' Cargo.toml)
  [ "$n" -eq 1 ] || die "expected exactly one top-level 'version =' in Cargo.toml, found $n"
  sed -n 's/^version = "\([^"]*\)"$/\1/p' Cargo.toml
}

lock_version() {
  awk '/^\[\[package\]\]/{p=0} /^name = "'"$CRATE"'"$/{p=1} p && /^version = /{gsub(/"/,"",$3); print $3; exit}' Cargo.lock
}

# true when $1 > $2 in semver order; a prerelease sorts before its release
version_gt() {
  local a="${1%%-*}" b="${2%%-*}" pa="${1#"${1%%-*}"}" pb="${2#"${2%%-*}"}"
  local -a A B
  IFS=. read -r -a A <<<"$a"
  IFS=. read -r -a B <<<"$b"
  local i
  for i in 0 1 2; do
    [ "${A[$i]}" -gt "${B[$i]}" ] && return 0
    [ "${A[$i]}" -lt "${B[$i]}" ] && return 1
  done
  [ -z "$pa" ] && [ -n "$pb" ] && return 0
  [ -n "$pa" ] && [ -z "$pb" ] && return 1
  [[ "$pa" > "$pb" ]]
}

last_tag() { git describe --tags --abbrev=0 --match 'v*' origin/main 2>/dev/null || true; }

require_clean_tree() {
  git diff --quiet && git diff --cached --quiet \
    || die "working tree is not clean; commit or stash first"
}

require_main_up_to_date() {
  say "fetching origin"
  git fetch --quiet --tags origin
  local branch
  branch=$(git rev-parse --abbrev-ref HEAD)
  [ "$branch" = "main" ] || die "run this from main (currently on '$branch')"
  [ "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)" ] \
    || die "main is not at origin/main; pull (or push) first"
}

tag_exists() {
  git rev-parse -q --verify "refs/tags/$1" >/dev/null 2>&1 && return 0
  [ -n "$(git ls-remote --tags origin "refs/tags/$1")" ]
}

crates_io_has() {
  curl -fsSL -A "$CRATE-release-script" "https://crates.io/api/v1/crates/$CRATE/$1" >/dev/null 2>&1
}

commits_since() {
  if [ -n "$1" ]; then git log --oneline "$1..origin/main"; else git log --oneline origin/main; fi
}

archive_name() {
  case "$1" in
    *windows*) echo "$CRATE-$1.zip" ;;
    *) echo "$CRATE-$1.tar.gz" ;;
  esac
}

# ---------------------------------------------------------------- status

cmd_status() {
  git fetch --quiet --tags origin
  local cur tag n
  cur=$(cargo_version)
  tag=$(last_tag)
  say "Cargo.toml version: $cur (Cargo.lock: $(lock_version))"
  say "last tag on main:   ${tag:-none}"
  n=$(commits_since "$tag" | wc -l | tr -d ' ')
  say "commits on origin/main since ${tag:-the beginning}: $n"
  commits_since "$tag" | sed 's/^/     /'
  if grep -q '^## \[Unreleased\]' CHANGELOG.md; then
    say "CHANGELOG has an [Unreleased] section"
  else
    warn "CHANGELOG has no [Unreleased] section: nothing is written up for the next release"
  fi
  if [ -n "$tag" ] && [ "v$cur" = "$tag" ] && [ "$n" -gt 0 ]; then
    warn "Cargo.toml still says $cur: the next release needs a bump (prepare X.Y.Z)"
  elif [ -n "$tag" ] && version_gt "$cur" "${tag#v}"; then
    say "bump to $cur is on main but not tagged yet: run 'tag $cur' once the PR is merged"
  fi
}

# ---------------------------------------------------------------- prepare

date_changelog() {
  local version="$1"
  grep -q '^## \[Unreleased\]$' CHANGELOG.md \
    || die "CHANGELOG.md has no '## [Unreleased]' heading to date"
  # the section must have content before the next version heading
  awk '/^## \[Unreleased\]$/{f=1; next} /^## \[/{exit} f && NF {found=1} END{exit !found}' CHANGELOG.md \
    || die "the [Unreleased] section of CHANGELOG.md is empty"
  sed -i -E "s/^## \[Unreleased\]$/## [$version] - $(date +%F)/" CHANGELOG.md
}

pin_docs() {
  local version="$1" f
  for f in "${PINNED_DOCS[@]}"; do
    grep -q "$CRATE@v[0-9][0-9.]*" "$f" || die "no action pin found in $f"
    sed -i -E "s/$CRATE@v[0-9]+\.[0-9]+\.[0-9]+/$CRATE@v$version/g" "$f"
  done
}

local_checks() {
  local version="$1"
  say "cargo fmt --check"
  cargo fmt --check
  say "cargo clippy --all-targets -- -D warnings"
  cargo clippy --all-targets --locked -- -D warnings
  say "cargo test --locked"
  cargo test --locked
  say "cargo publish --dry-run (validates the crates.io package)"
  cargo publish --dry-run --locked --allow-dirty
  say "cargo build --release --locked (the shots and the smoke below use it)"
  cargo build --release --locked
  local out
  out=$(target/release/$CRATE --version)
  [[ "$out" == *"$version"* ]] || die "release binary reports '$out', expected $version"

  local maturin=".venv/bin/maturin"
  [ -x "$maturin" ] || maturin=$(command -v maturin || true)
  if [ -z "$maturin" ]; then
    warn "maturin not found: skipping the wheel build (CI's wheel-smoke covers it)"
    return
  fi
  say "maturin build --release (wheel + clean-venv smoke)"
  rm -rf target/wheels-check && "$maturin" build --release --locked -o target/wheels-check >/dev/null
  local venv
  venv=$(mktemp -d "${TMPDIR:-/tmp}/dx-wheel-XXXX")
  python3 -m venv "$venv" && "$venv/bin/pip" install --quiet target/wheels-check/*.whl
  out=$("$venv/bin/$CRATE" --version)
  [[ "$out" == *"$version"* ]] || die "wheel binary reports '$out', expected $version"
  "$venv/bin/python" -c "import delta_explain" || die "import delta_explain failed in the clean venv"
  rm -rf "$venv"
  ok "wheel installs clean and reports $version"
}

cmd_prepare() {
  local version="$1" dry_run="$2" skip_checks="$3"
  local tag="v$version" branch="release/$version" current prev n body

  check_version_syntax "$version"
  require_clean_tree
  require_main_up_to_date

  current=$(cargo_version)
  version_gt "$version" "$current" \
    || die "$version is not greater than the current version $current"
  tag_exists "$tag" && die "tag $tag already exists"
  git rev-parse -q --verify "refs/heads/$branch" >/dev/null \
    && die "branch $branch already exists locally"
  [ -z "$(git ls-remote --heads origin "refs/heads/$branch")" ] \
    || die "branch $branch already exists on origin"
  if ! is_prerelease "$version" && crates_io_has "$version"; then
    die "$CRATE $version is already on crates.io"
  fi
  gh auth status >/dev/null 2>&1 || die "gh is not authenticated (gh auth login)"

  prev=$(last_tag)
  n=$(commits_since "$prev" | wc -l | tr -d ' ')
  [ "$n" -gt 0 ] || die "no commits on main since $prev; nothing to release"
  say "releasing $version (current $current, last tag ${prev:-none}, $n commits since)"

  say "creating branch $branch"
  git switch --quiet -c "$branch"

  say "bumping Cargo.toml $current -> $version"
  sed -i -E "s/^version = \"[^\"]+\"/version = \"$version\"/" Cargo.toml
  [ "$(cargo_version)" = "$version" ] || die "Cargo.toml bump failed"
  say "syncing Cargo.lock"
  cargo update --quiet --workspace --offline
  [ "$(lock_version)" = "$version" ] || die "Cargo.lock does not carry $version after cargo update"

  say "dating the changelog"
  date_changelog "$version"
  say "pinning $CRATE@$tag in ${PINNED_DOCS[*]}"
  pin_docs "$version"

  local changed
  changed=$(git diff --name-only | LC_ALL=C sort | tr '\n' ' ')
  [ "$changed" = "CHANGELOG.md Cargo.lock Cargo.toml README.md book/src/guides/ci-gating.md " ] \
    || die "unexpected set of files changed by the bump: $changed"

  if [ "$skip_checks" = 1 ]; then
    warn "skipping local checks (--skip-checks); CI runs them on the PR"
  else
    local_checks "$version"
  fi

  say "committing"
  git add Cargo.toml Cargo.lock CHANGELOG.md "${PINNED_DOCS[@]}"
  git commit --quiet -m "chore: release as $version"
  ok "committed $(git rev-parse --short HEAD) on $branch"

  body=$(cat <<EOF
Version bump, dated changelog and action pin for $version: $n commits on main since ${prev:-the first commit}. Tagging \`$tag\` after the merge triggers release.yml and docker.yml.
EOF
)

  if [ "$dry_run" = 1 ]; then
    warn "dry run: not pushing. To continue by hand:"
    printf '     git push -u origin %s\n' "$branch"
    printf '     gh pr create --base main --title "chore: release as %s" --body "%s"\n' "$version" "$body"
  else
    say "pushing $branch"
    git push --quiet -u origin "$branch"
    say "opening the PR"
    gh pr create --base main --head "$branch" --title "chore: release as $version" --body "$body"
  fi

  cat <<EOF

Still to do on $branch before the merge (RELEASE.md, section 1):
  - differential harness, fresh mode (examples/differential up):
      DX_BIN=\$PWD/target/release/$CRATE DX_DIFF_FRESH=1 python3 examples/differential/run_differential.py
  - README terminal shots against this build (catalog stack up, see the script):
      python3 scripts/render_readme_shots.py
  - if the report shape or the CLI output changed: re-run examples/taxi-optimization
    and refresh viewer/screenshot.png
  - read README.md once more: crates.io freezes it at publish time
Then wait for full CI (docker-smoke included), merge, and run: scripts/release.sh tag $version
EOF
}

# ---------------------------------------------------------------- tag

cmd_tag() {
  local version="$1" dry_run="$2" watch="$3"
  local tag="v$version"

  check_version_syntax "$version"
  require_clean_tree
  if [ "$(git rev-parse --abbrev-ref HEAD)" != "main" ]; then
    say "switching to main"
    git switch --quiet main
  fi
  git fetch --quiet --tags origin
  git pull --quiet --ff-only origin main
  require_main_up_to_date

  [ "$(cargo_version)" = "$version" ] \
    || die "Cargo.toml on main says $(cargo_version), not $version; has the release PR been merged?"
  [ "$(lock_version)" = "$version" ] \
    || die "Cargo.lock on main says $(lock_version), not $version"
  grep -q "^## \[$version\] - " CHANGELOG.md \
    || die "CHANGELOG.md has no dated heading for $version"
  tag_exists "$tag" && die "tag $tag already exists"
  is_prerelease "$version" && warn "$version is a prerelease"

  say "tagging $(git rev-parse --short HEAD) ($(git log -1 --format=%s)) as $tag"
  if [ "$dry_run" = 1 ]; then
    warn "dry run: not tagging. To continue by hand:"
    printf '     git tag -a %s -m "%s %s" && git push origin %s\n' "$tag" "$CRATE" "$version" "$tag"
    return
  fi
  git tag -a "$tag" -m "$CRATE $version"
  git push --quiet origin "$tag"
  ok "pushed $tag; release.yml and docker.yml are starting: https://github.com/$REPO/actions"

  if [ "$watch" = 0 ]; then
    warn "not watching (--no-watch); later run: scripts/release.sh verify $version"
    return
  fi
  watch_workflow release.yml "$tag"
  watch_workflow docker.yml "$tag"
  cmd_verify "$version"
}

watch_workflow() {
  local workflow="$1" tag="$2" run_id=""
  say "waiting for the $workflow run on $tag"
  for _ in $(seq 1 30); do
    run_id=$(gh run list --repo "$REPO" --workflow="$workflow" --branch "$tag" \
               --json databaseId --limit 1 -q '.[0].databaseId' 2>/dev/null || true)
    [ -n "$run_id" ] && break
    sleep 5
  done
  [ -n "$run_id" ] || die "no $workflow run appeared for $tag after 150s; check the Actions tab"
  say "watching $workflow run $run_id"
  gh run watch --repo "$REPO" --exit-status "$run_id" \
    || die "the $workflow run failed: gh run view --repo $REPO $run_id --log-failed"
  ok "$workflow run $run_id succeeded"
}

# ---------------------------------------------------------------- verify

cmd_verify() {
  local version="$1" failures=0
  local tag="v$version"
  check_version_syntax "$version"
  local tmp
  tmp=$(mktemp -d "${TMPDIR:-/tmp}/dx-verify-XXXX")

  say "GitHub release $tag"
  local assets expected t
  assets=$(gh release view --repo "$REPO" "$tag" --json assets,isDraft \
             -q 'if .isDraft then error("release is still a draft") else .assets[].name end') \
    || { warn "release $tag not found or still a draft"; failures=$((failures+1)); assets=""; }
  for t in "${TARGETS[@]}"; do
    for expected in "$(archive_name "$t")" "$CRATE-$t.sha256"; do
      if grep -qx "$expected" <<<"$assets"; then ok "$expected"
      else warn "missing asset: $expected"; failures=$((failures+1)); fi
    done
  done
  for expected in "${CRATE}_${version}-1_amd64.deb" "${CRATE}_${version}-1_arm64.deb"; do
    if grep -qx "$expected" <<<"$assets"; then ok "$expected"
    else warn "missing asset: $expected"; failures=$((failures+1)); fi
  done

  # the published checksum of an archive, from its .sha256 asset
  published_sha() {
    gh release download --repo "$REPO" "$tag" -p "$CRATE-$1.sha256" -D "$tmp" --clobber >/dev/null 2>&1 \
      && awk '{print $1}' "$tmp/$CRATE-$1.sha256"
  }

  if is_prerelease "$version"; then
    warn "prerelease: skipping tap, Scoop, crates.io and PyPI checks"
  else
    say "Homebrew tap Formula/$CRATE.rb"
    local formula formula_version sha
    formula=$(gh api "repos/$TAP_REPO/contents/Formula/$CRATE.rb" -q .content | base64 -d) || formula=""
    formula_version=$(sed -n 's/^ *version "\([^"]*\)"$/\1/p' <<<"$formula")
    if [ "$formula_version" = "$version" ]; then ok "formula at $version"
    else warn "formula says '${formula_version:-?}', expected $version"; failures=$((failures+1)); fi
    for t in "${TAP_TARGETS[@]}"; do
      sha=$(grep -A1 "$tag/$CRATE-$t.tar.gz" <<<"$formula" | sed -n 's/^ *sha256 "\([0-9a-f]*\)"$/\1/p')
      if [ -n "$sha" ] && [ "$sha" = "$(published_sha "$t")" ]; then ok "formula sha256 for $t matches the release"
      else warn "formula sha256 for $t is '${sha:-missing}', not the published one"; failures=$((failures+1)); fi
    done

    say "Scoop bucket/$CRATE.json"
    local manifest
    manifest=$(gh api "repos/$SCOOP_REPO/contents/bucket/$CRATE.json" -q .content | base64 -d) || manifest="{}"
    if [ "$(jq -r .version <<<"$manifest")" = "$version" ]; then ok "manifest at $version"
    else warn "manifest says '$(jq -r .version <<<"$manifest")', expected $version"; failures=$((failures+1)); fi
    sha=$(jq -r '.architecture["64bit"].hash' <<<"$manifest")
    if [ "$sha" = "$(published_sha x86_64-pc-windows-msvc)" ]; then ok "manifest hash matches the windows zip"
    else warn "manifest hash '$sha' is not the published one"; failures=$((failures+1)); fi

    say "crates.io"
    local crate_json
    if crate_json=$(curl -fsSL -A "$CRATE-release-script" "https://crates.io/api/v1/crates/$CRATE/$version"); then
      ok "$CRATE $(jq -r .version.num <<<"$crate_json") published $(jq -r .version.created_at <<<"$crate_json")"
    else warn "$CRATE $version is not on crates.io"; failures=$((failures+1)); fi

    say "PyPI"
    local pypi_json
    if pypi_json=$(curl -fsSL "https://pypi.org/pypi/$CRATE/$version/json"); then
      ok "$CRATE $version on PyPI with $(jq '.urls | length' <<<"$pypi_json") files"
    else warn "$CRATE $version is not on PyPI"; failures=$((failures+1)); fi
  fi

  say "ghcr.io image $IMAGE:$version"
  if command -v docker >/dev/null; then
    local platforms
    platforms=$(docker manifest inspect "$IMAGE:$version" 2>/dev/null \
                  | jq -r '.manifests[]?.platform | select(.os == "linux") | .architecture' | sort | tr '\n' ' ')
    if [[ "$platforms" == *amd64* && "$platforms" == *arm64* ]]; then ok "manifests: $platforms"
    else warn "image manifests found: '${platforms:-none}', expected amd64 and arm64"; failures=$((failures+1)); fi
  else
    warn "docker not found: skipping the image check"
  fi

  if [ "$(uname -s)" = Linux ] && [ "$(uname -m)" = x86_64 ] && [ -n "$assets" ]; then
    say "smoke test of the linux x86_64 tarball"
    local target="x86_64-unknown-linux-gnu" archive="$CRATE-x86_64-unknown-linux-gnu.tar.gz" root
    root=$(git rev-parse --show-toplevel)
    (
      cd "$tmp"
      gh release download --repo "$REPO" "$tag" -p "$archive" -p "$CRATE-$target.sha256" --clobber >/dev/null
      sha256sum --quiet -c "$CRATE-$target.sha256"
      tar -xzf "$archive"
      out=$("./$CRATE" --version)
      [[ "$out" == *"$version"* ]] || { echo "$CRATE --version printed '$out'"; exit 1; }
      "./$CRATE" "$root/fixtures/test-table" -w "country = 'DE'" --format json \
        | jq -e --arg v "$version" '.tool_version == $v and .final_files == 2' >/dev/null
    ) && ok "sha256 ok, --version reports $version, the fixture prunes as expected" \
      || { warn "tarball smoke test failed (see above)"; failures=$((failures+1)); }
  fi

  if ! is_prerelease "$version"; then
    say "pip install $CRATE==$version in a clean venv"
    local venv="$tmp/venv"
    if python3 -m venv "$venv" && "$venv/bin/pip" install --quiet "$CRATE==$version" \
       && [[ "$("$venv/bin/$CRATE" --version)" == *"$version"* ]] \
       && "$venv/bin/python" -c "import delta_explain"; then
      ok "wheel installs from PyPI and reports $version"
    else warn "pip smoke test failed (PyPI may still be propagating)"; failures=$((failures+1)); fi
  fi

  rm -rf "$tmp"
  if [ "$failures" -eq 0 ]; then
    ok "release $tag verified"
  else
    die "$failures check(s) failed for $tag"
  fi
}

# ---------------------------------------------------------------- main

main() {
  need_tools
  cd "$(git rev-parse --show-toplevel)"

  local cmd="${1:-}" version="" dry_run=0 skip_checks=0 watch=1
  [ -n "$cmd" ] || usage 1
  shift
  while [ $# -gt 0 ]; do
    case "$1" in
      --dry-run) dry_run=1 ;;
      --skip-checks) skip_checks=1 ;;
      --no-watch) watch=0 ;;
      -h|--help) usage ;;
      -*) die "unknown option: $1" ;;
      *) [ -z "$version" ] || die "unexpected argument: $1"; version="${1#v}" ;;
    esac
    shift
  done

  case "$cmd" in
    status) cmd_status ;;
    prepare) [ -n "$version" ] || die "prepare needs a version"; cmd_prepare "$version" "$dry_run" "$skip_checks" ;;
    tag) [ -n "$version" ] || die "tag needs a version"; cmd_tag "$version" "$dry_run" "$watch" ;;
    verify) [ -n "$version" ] || die "verify needs a version"; cmd_verify "$version" ;;
    -h|--help|help) usage ;;
    *) die "unknown command: $cmd (status, prepare, tag, verify)" ;;
  esac
}

main "$@"
