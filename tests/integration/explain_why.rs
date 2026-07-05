//! `--explain-why` (issue: the v0.5 diagnostic layer, ADR 0007): the
//! deterministic diagnostic engine that turns the report into advice. Text
//! section under the flag, additive `explain` array in JSON.

use crate::common::{cmd, fixture};
use predicates::prelude::*;

#[test]
fn diagnoses_a_predicate_that_misses_the_partition_column() {
    // taxi-nyc is partitioned by pickup_date; a fare predicate touches no
    // partition column and its zone/fare stats do not prune here.
    cmd()
        .args([
            &fixture("taxi-nyc"),
            "-w",
            "fare_amount > 100",
            "--explain-why",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Why:")
                .and(predicate::str::contains("[NO_PARTITION_FILTER]"))
                .and(predicate::str::contains("Filter on a partition column"))
                .and(predicate::str::contains("[WEAK_DATA_SKIPPING]")),
        );
}

#[test]
fn a_well_pruning_predicate_reports_no_issues() {
    cmd()
        .args([
            &fixture("taxi-nyc"),
            "-w",
            "pickup_date = '2024-01-03'",
            "--explain-why",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("No pruning issues found"));
}

#[test]
fn without_the_flag_there_is_no_why_section() {
    cmd()
        .args([&fixture("taxi-nyc"), "-w", "fare_amount > 100"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Why:").not());
}

#[test]
fn unsupported_fragment_becomes_advice() {
    cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "UPPER(name) = 'X'",
            "--explain-why",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("[UNSUPPORTED_FRAGMENT]")
                .and(predicate::str::contains("outside the pruning language")),
        );
}

#[test]
fn json_carries_the_explain_array_only_with_the_flag() {
    // with the flag: array present, entries well-formed
    let out = cmd()
        .args([
            &fixture("taxi-nyc"),
            "-w",
            "fare_amount > 100",
            "--explain-why",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let explain = json["explain"].as_array().expect("explain array");
    assert!(!explain.is_empty());
    let codes: Vec<&str> = explain
        .iter()
        .map(|d| d["code"].as_str().unwrap())
        .collect();
    assert!(codes.contains(&"NO_PARTITION_FILTER"));
    assert_eq!(explain[0]["severity"], "warning");

    // without the flag: no explain key (byte-stable compact document)
    let out = cmd()
        .args([
            &fixture("taxi-nyc"),
            "-w",
            "fare_amount > 100",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(json.get("explain").is_none());
}
