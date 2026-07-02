//! Integration tests for the `test-table-checkpointed` fixture.
//!
//! The fixture simulates a long-lived production table after log cleanup:
//! three appends, a checkpoint at v2, and every JSON commit deleted. All
//! surviving `add` actions (and their stats) live only inside the checkpoint
//! Parquet. Per-file statistics are therefore only reachable through the
//! kernel's log replay: reading the JSON commits directly finds nothing, and
//! before stats were sourced from the kernel scan these tests failed with
//! every file rendered as `[no stats]` and `--assert-stats` exiting 1.

use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn checkpointed_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/test-table-checkpointed")
}

// ── Fixture sanity ──────────────────────────────────────────────────

#[test]
fn fixture_reports_three_files_from_checkpoint_only_log() {
    cmd()
        .arg(checkpointed_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 3"));
}

// ── Stats come from the kernel's checkpoint replay ──────────────────

#[test]
fn verbose_shows_stats_for_checkpoint_only_files() {
    cmd()
        .args([&checkpointed_table(), "-w", "age > 55", "--verbose"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("stats(age: 60..70")
                .and(predicate::str::contains("stats(age: 20..30"))
                .and(predicate::str::contains("[no stats]").not()),
        );
}

#[test]
fn data_skipping_prunes_on_checkpoint_stats() {
    cmd()
        .args([&checkpointed_table(), "-w", "age > 55"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 1  (-2, 67% pruned)",
        ));
}

// ── --assert-stats no longer false-positives on checkpointed logs ───

#[test]
fn assert_stats_passes_on_checkpoint_only_log() {
    cmd()
        .args([&checkpointed_table(), "--assert-stats"])
        .assert()
        .success();
}

#[test]
fn json_stats_mode_is_exact_on_checkpoint_only_log() {
    let output = cmd()
        .args([&checkpointed_table(), "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats"]["mode"], "exact");
    assert_eq!(json["stats"]["files_with_stats"], 3);
}
