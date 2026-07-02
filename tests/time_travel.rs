//! Integration tests for `--at-version` (time travel).
//!
//! `test-table` is built as one overwrite plus five appends, so versions 0..5
//! hold 1..6 files. Analyzing an old version must replay the log only up to
//! that version: fewer files in the snapshot, pruning computed against the
//! historical layout.

use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn test_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/test-table")
}

#[rstest]
#[case(0, "Files in snapshot: 1")]
#[case(2, "Files in snapshot: 3")]
#[case(5, "Files in snapshot: 6")]
fn snapshot_reflects_requested_version(#[case] version: u64, #[case] expected: &str) {
    cmd()
        .args([&test_table(), "--at-version", &version.to_string()])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(format!("Version:     {version}"))
                .and(predicate::str::contains(expected)),
        );
}

#[test]
fn pruning_runs_against_the_historical_layout() {
    // At version 2 only the two DE files and one US file exist; the partition
    // fragment prunes the US file, data skipping drops the low-age DE file.
    cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE' AND age > 40",
            "--at-version",
            "2",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Files in snapshot: 3")
                .and(predicate::str::contains("Phase 1: Partition pruning")),
        );
}

#[test]
fn future_version_fails_cleanly() {
    cmd()
        .args([&test_table(), "--at-version", "99"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn without_flag_latest_version_is_used() {
    cmd()
        .arg(test_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Version:     5"));
}
