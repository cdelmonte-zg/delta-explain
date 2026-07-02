//! Integration tests for the checkpoint-only fixtures.
//!
//! Both fixtures simulate a long-lived production table after log cleanup:
//! three appends, a checkpoint at v2, and every JSON commit deleted. All
//! surviving `add` actions (and their stats) live only inside the checkpoint
//! Parquet. Per-file statistics are therefore only reachable through the
//! kernel's log replay: reading the JSON commits directly finds nothing, and
//! before stats were sourced from the kernel scan these tests failed with
//! every file rendered as `[no stats]` and `--assert-stats` exiting 1.
//!
//! The two variants cover the two checkpoint layouts:
//! - `test-table-checkpointed`: the checkpoint carries `add.stats` JSON
//!   (deltalake's default, `writeStatsAsJson=true`).
//! - `test-table-checkpointed-struct`: the checkpoint carries only the
//!   structured `stats_parsed` column (`writeStatsAsJson=false`). This one
//!   requires requesting the parsed stats schema from the kernel
//!   (`include_all_stats_columns`), which makes the kernel populate the scan
//!   row's `stats` field via COALESCE(add.stats, ToJson(add.stats_parsed)).

use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn table(fixture: &str) -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/{fixture}")
}

// ── Fixture sanity ──────────────────────────────────────────────────

#[rstest]
#[case("test-table-checkpointed")]
#[case("test-table-checkpointed-struct")]
fn fixture_reports_three_files_from_checkpoint_only_log(#[case] fixture: &str) {
    cmd()
        .arg(table(fixture))
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 3"));
}

// ── Stats come from the kernel's checkpoint replay ──────────────────

#[rstest]
#[case("test-table-checkpointed")]
#[case("test-table-checkpointed-struct")]
fn verbose_shows_stats_for_checkpoint_only_files(#[case] fixture: &str) {
    cmd()
        .args([&table(fixture), "-w", "age > 55", "--verbose"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("stats(age: 60..70")
                .and(predicate::str::contains("stats(age: 20..30"))
                .and(predicate::str::contains("2 records"))
                .and(predicate::str::contains("[no stats]").not()),
        );
}

#[rstest]
#[case("test-table-checkpointed")]
#[case("test-table-checkpointed-struct")]
fn data_skipping_prunes_on_checkpoint_stats(#[case] fixture: &str) {
    cmd()
        .args([&table(fixture), "-w", "age > 55"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 1  (-2, 67% pruned)",
        ));
}

// ── --assert-stats no longer false-positives on checkpointed logs ───

#[rstest]
#[case("test-table-checkpointed")]
#[case("test-table-checkpointed-struct")]
fn assert_stats_passes_on_checkpoint_only_log(#[case] fixture: &str) {
    cmd()
        .args([&table(fixture), "--assert-stats"])
        .assert()
        .success();
}

#[rstest]
#[case("test-table-checkpointed")]
#[case("test-table-checkpointed-struct")]
fn json_stats_mode_is_exact_on_checkpoint_only_log(#[case] fixture: &str) {
    let output = cmd()
        .args([&table(fixture), "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats"]["mode"], "exact");
    assert_eq!(json["stats"]["files_with_stats"], 3);
}
