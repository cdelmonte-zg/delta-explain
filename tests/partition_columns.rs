use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn fixture(name: &str) -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/{name}")
}

// ── Partition column detection from metadata ───────────────────────

/// Partitioned table: partition pruning phase must appear when filtering
/// on the partition column, proving that partitionColumns were detected.
#[rstest]
#[case("country = 'DE'", "Partition pruning", 2)]
#[case("country = 'US'", "Partition pruning", 2)]
#[case("country = 'IT'", "Partition pruning", 2)]
fn partitioned_table_detects_partition_column(
    #[case] predicate: &str,
    #[case] expected_phase: &str,
    #[case] expected_remaining: usize,
) {
    cmd()
        .args([&fixture("test-table"), "-w", predicate])
        .assert()
        .success()
        .stdout(
            predicate::str::contains(expected_phase).and(predicate::str::contains(format!(
                "files remaining: {expected_remaining}"
            ))),
        );
}

/// Flat table: no partition columns, so partition pruning must never appear.
#[rstest]
#[case("country = 'DE'")]
#[case("age > 40")]
#[case("country = 'DE' AND age > 40")]
fn flat_table_has_no_partition_phase(#[case] predicate: &str) {
    cmd()
        .args([&fixture("test-table-flat"), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains("Partition pruning").not());
}

/// Combined predicate on partitioned table: both phases must appear.
#[rstest]
#[case("country = 'DE' AND age > 40", 2, 1)]
#[case("country = 'US' AND age > 30", 2, 1)]
fn partitioned_table_both_phases(
    #[case] predicate: &str,
    #[case] expected_phases: usize,
    #[case] expected_final: usize,
) {
    let output = cmd()
        .args([&fixture("test-table"), "-w", predicate, "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), expected_phases);
    assert_eq!(phases[0]["name"], "Partition pruning");
    assert_eq!(phases[1]["name"], "Data skipping (min/max statistics)");
    assert_eq!(json["final_files"], expected_final);
}

// ── Empty table (zero files, metadata present) ─────────────────────

#[test]
fn empty_table_reports_zero_files() {
    cmd()
        .arg(&fixture("test-table-empty"))
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 0"));
}

#[test]
fn empty_table_with_predicate_succeeds() {
    cmd()
        .args([&fixture("test-table-empty"), "-w", "region = 'EU'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 0"));
}

#[test]
fn empty_table_json_reports_zero() {
    let output = cmd()
        .args([&fixture("test-table-empty"), "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["total_files"], 0);
}

// ── Partition columns must come from metadata, not file inference ───

/// The empty table is partitioned by "region" according to its metadata.
/// Even with zero files, partition columns should be known — so a predicate
/// on "region" should trigger a partition pruning phase (with 0 files in/out).
#[test]
fn empty_table_knows_partition_columns_from_metadata() {
    // This test will FAIL with the old inference-from-first-file approach
    // because there are no files to infer from.
    cmd()
        .args([&fixture("test-table-empty"), "-w", "region = 'EU'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Partition pruning"));
}

/// JSON output for empty table with partition predicate should show
/// a partition_pruning phase.
#[test]
fn empty_table_json_partition_phase() {
    let output = cmd()
        .args([
            &fixture("test-table-empty"),
            "-w",
            "region = 'EU'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let phases = json["phases"].as_array().unwrap();
    assert!(
        phases.iter().any(|p| p["name"] == "Partition pruning"),
        "expected a Partition pruning phase, got: {phases:?}"
    );
}

// ── Regression: data-only predicates must not create partition phase ──

#[rstest]
#[case("test-table", "age > 30")]
#[case("test-table", "score > 90.0")]
#[case("test-table-flat", "age > 30")]
fn data_only_predicate_skips_partition_phase(#[case] table: &str, #[case] predicate: &str) {
    // When the predicate only references non-partition columns,
    // no partition pruning phase should appear.
    let output = cmd()
        .args([&fixture(table), "-w", predicate, "--format", "json"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let phases = json["phases"].as_array().unwrap();
    assert!(
        !phases.iter().any(|p| p["name"] == "Partition pruning"),
        "unexpected Partition pruning phase for data-only predicate"
    );
}
