//! The `taxi-nyc` fixture: a real Delta table (deltalake-written) from NYC
//! TLC yellow-taxi data, partitioned by pickup date. Unlike the synthetic
//! fixtures, its log carries the partition layout and per-file statistics a
//! real writer produces - this module pins that delta-explain reads that
//! shape correctly, on both pruning axes.

use crate::common::{cmd, fixture};
use predicates::prelude::*;

fn taxi() -> String {
    fixture("taxi-nyc")
}

#[test]
fn reads_the_real_table_and_reports_five_date_partitions() {
    cmd()
        .arg(taxi())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 5"));
}

#[test]
fn date_predicate_prunes_partitions_exactly() {
    // pickup_date is the partition column: one file per day, exact pruning.
    cmd()
        .args([&taxi(), "-w", "pickup_date = '2024-01-03'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: pickup_date = '2024-01-03'")
                .and(predicate::str::contains("files remaining: 1"))
                .and(predicate::str::contains("confidence:     exact")),
        );
}

#[test]
fn date_and_fare_split_across_both_phases() {
    cmd()
        .args([
            &taxi(),
            "-w",
            "pickup_date = '2024-01-03' AND fare_amount > 50",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: pickup_date = '2024-01-03'")
                .and(predicate::str::contains("stats-safe:     fare_amount > 50"))
                .and(predicate::str::contains("Phase 1: Partition pruning"))
                .and(predicate::str::contains(
                    "Phase 2: Data skipping (min/max statistics)",
                )),
        );
}

#[test]
fn real_writer_stats_are_present_on_every_file() {
    // deltalake records stats; --assert-stats must pass on the real table.
    cmd().args([&taxi(), "--assert-stats"]).assert().success();
}

#[test]
fn fare_predicate_is_stats_safe_and_json_carries_the_analysis() {
    let output = cmd()
        .args([&taxi(), "-w", "fare_amount > 100", "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(json["analysis"]["stats_safe"], "fare_amount > 100");
    assert!(json["analysis"]["partition_safe"].is_null());
    assert_eq!(json["stats"]["mode"], "exact");
    assert_eq!(json["total_files"], 5);
}
