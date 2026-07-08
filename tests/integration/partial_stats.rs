//! Integration tests for the `test-table-partial-stats` fixture (Step 1.4).
//!
//! The fixture is built with 4 files, 2 of which have their `stats` field
//! stripped from the Delta log. It exercises the `stats.mode = "partial"`
//! code path that no other fixture currently covers.

use crate::common::{cmd, fixture};
use predicates::prelude::*;
use rstest::rstest;

fn partial_table() -> String {
    fixture("test-table-partial-stats")
}

// ── Fixture sanity ──────────────────────────────────────────────────

#[test]
fn fixture_reports_four_files() {
    cmd()
        .arg(partial_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 4"));
}

// ── stats.mode = "partial" path ─────────────────────────────────────

#[test]
fn json_stats_mode_is_partial() {
    let output = cmd()
        .args([&partial_table(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(json["stats"]["mode"], "partial");
    assert_eq!(json["stats"]["files_with_stats"], 2);
    assert_eq!(json["stats"]["total_files"], 4);
    let pct = json["stats"]["pct"].as_f64().unwrap();
    assert!((pct - 50.0).abs() < 0.01, "expected 50%, got {pct}");
}

/// `--assert-stats` must fail on the partial fixture: 2 files lack stats.
#[test]
fn assert_stats_fails_on_partial_fixture() {
    cmd()
        .args([&partial_table(), "--assert-stats"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "ASSERTION FAILED: 2 file(s) missing statistics",
        ));
}

#[test]
fn json_assertion_stats_complete_reports_missing_count() {
    let output = cmd()
        .args([&partial_table(), "--format", "json", "--assert-stats"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["assertions"][0]["name"], "stats_complete");
    assert_eq!(json["assertions"][0]["missing_count"], 2);
    assert_eq!(json["assertions"][0]["result"], "fail");
    assert_eq!(json["result"], "fail");
}

// ── Pruning behaviour with partial stats ────────────────────────────

/// Partition pruning must work normally regardless of stats coverage -
/// it operates on partition values, not on min/max statistics.
#[rstest]
#[case("country = 'DE'", 2)]
#[case("country = 'IT'", 2)]
fn partition_pruning_unaffected_by_partial_stats(
    #[case] predicate: &str,
    #[case] expected_remaining: usize,
) {
    cmd()
        .args([&partial_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "files remaining: {expected_remaining}"
        )));
}

/// Data-skipping on a stats column: files with stats can be eliminated,
/// files without stats are kept conservatively.
#[test]
fn data_skipping_keeps_files_without_stats() {
    let output = cmd()
        .args([&partial_table(), "-w", "age > 100", "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    // age > 100 should eliminate every file with stats. Files without stats
    // are kept because the kernel cannot prove their min/max excludes the
    // predicate.
    let final_files = json["final_files"].as_u64().unwrap();
    assert_eq!(
        final_files, 2,
        "expected 2 files (the ones without stats) to survive `age > 100`, got {final_files}"
    );
}
