//! Integration tests for the `test-table-nested` fixture: data skipping and
//! stats display on nested (struct) columns addressed by dotted paths.
//!
//! The fixture has a `profile` struct column with an int leaf (age) and a
//! double leaf (score). Delta records min/max for nested leaves, so the kernel
//! can skip files on `profile.age` / `profile.score`.

use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn nested_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/test-table-nested")
}

#[test]
fn fixture_reports_six_files() {
    cmd()
        .arg(nested_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 6"));
}

// ── data skipping on nested columns ─────────────────────────────────

#[rstest]
#[case("profile.age > 40", 3)] // int leaf
#[case("profile.score > 90", 2)] // double leaf
#[case("profile.score > 200", 0)]
#[case("profile.geo.zip > 3500", 3)] // leaf two levels deep
#[case("country = 'DE' AND profile.age > 40", 1)] // partition + nested stats
fn nested_pruning(#[case] predicate: &str, #[case] remaining: usize) {
    cmd()
        .args([&nested_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "files remaining: {remaining}"
        )));
}

/// A nested double column compared to an integer literal must coerce to the
/// leaf type, not abort with "Invalid comparison operation: Float64 > Int32".
#[test]
fn nested_double_compared_to_int_literal_does_not_error() {
    cmd()
        .args([&nested_table(), "-w", "profile.score > 90"])
        .assert()
        .success()
        .stderr(predicate::str::contains("Invalid comparison").not());
}

// ── flattened stats display ─────────────────────────────────────────

/// Verbose output reports each struct leaf under its dotted key, never the raw
/// struct object blob (`profile: {"age":...}`).
#[test]
fn verbose_shows_flattened_nested_stats() {
    cmd()
        .args([&nested_table(), "-w", "profile.age > 40", "--verbose"])
        .assert()
        .success()
        .stdout(predicate::str::contains("profile.age:"))
        .stdout(predicate::str::contains("profile.score:"))
        .stdout(predicate::str::contains("profile.geo.zip:")) // two levels deep
        .stdout(predicate::str::contains("profile: {").not());
}

/// A predicate on a nested (non-partition) column is classified as stats-safe.
#[test]
fn nested_predicate_is_stats_safe() {
    let output = cmd()
        .args([
            &nested_table(),
            "-w",
            "profile.age > 40",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(json["analysis"]["stats_safe"], "profile.age > 40");
    assert_eq!(json["analysis"]["confidence"], "conservative");
}
