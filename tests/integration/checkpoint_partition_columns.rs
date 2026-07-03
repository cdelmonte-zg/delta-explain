//! Integration tests for the `test-table-checkpointed-part` fixture.
//!
//! A partitioned table whose log is fully checkpointed: no JSON commits
//! remain, so no `metaData` action is readable and partition columns must be
//! derived from the `partitionValues` keys the kernel replays out of the
//! checkpoint. Before that fallback existed the predicate was classified
//! entirely stats-safe and the two-phase attribution collapsed into a single
//! data-skipping phase (total counts stayed correct).

use crate::common::{cmd, fixture};
use predicates::prelude::*;

fn table() -> String {
    fixture("test-table-checkpointed-part")
}

#[test]
fn fixture_reports_six_files_from_checkpoint_only_log() {
    cmd()
        .arg(table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 6"));
}

#[test]
fn partition_fragment_is_classified_from_checkpoint_partition_values() {
    cmd()
        .args([&table(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country = 'DE'")
                .and(predicate::str::contains("stats-safe:     age > 40")),
        );
}

#[test]
fn attribution_keeps_two_phases_on_checkpoint_only_log() {
    cmd()
        .args([&table(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning [exact]")
                .and(predicate::str::contains(
                    "files remaining: 2  (-4, 67% pruned)",
                ))
                .and(predicate::str::contains(
                    "Phase 2: Data skipping (min/max statistics)",
                ))
                .and(predicate::str::contains(
                    "files remaining: 1  (-1, 50% pruned)",
                ))
                .and(predicate::str::contains("83% pruned")),
        );
}

#[test]
fn json_analysis_carries_partition_fragment() {
    let output = cmd()
        .args([
            &table(),
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["analysis"]["partition_safe"], "country = 'DE'");
    assert_eq!(json["phases"].as_array().unwrap().len(), 2);
    assert_eq!(json["total_pruning_pct"].as_f64().unwrap().round(), 83.0);
}
