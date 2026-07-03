//! Proves the `LogBuilder` path: pruning semantics on a synthesized log,
//! including a scale smoke well beyond what checked-in fixtures cover.
//! delta-explain reads only the log, so none of the referenced parquet
//! files exist.

use predicates::prelude::*;

use crate::common::{LogBuilder, cmd, int_range_stats};

fn thousand_file_table() -> crate::common::TempTable {
    // 1000 files across 10 country partitions; age ranges tile [i, i+10]
    // so a high bound isolates a predictable tail of files.
    LogBuilder::new()
        .partition_column("country", "string")
        .column("age", "integer")
        .add_files(1000, |i| {
            let country = format!("C{}", i % 10);
            (
                format!("country={country}/part-{i:05}.parquet"),
                vec![("country".into(), country.clone())],
                Some(int_range_stats(
                    "age",
                    (i % 100) as i64,
                    (i % 100 + 10) as i64,
                    1000,
                )),
            )
        })
        .build()
}

#[test]
fn partition_pruning_scales_to_a_thousand_files() {
    let table = thousand_file_table();
    cmd()
        .args([&table.path(), "-w", "country = 'C3'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Files in snapshot: 1,000")
                .and(predicate::str::contains("files remaining: 100"))
                .and(predicate::str::contains("90% pruned")),
        );
}

#[test]
fn data_skipping_prunes_on_synthetic_stats() {
    let table = thousand_file_table();
    // age ranges tile [i%100, i%100+10]. A file can match age > 105 only if
    // its max exceeds 105: i%100 in 96..=99, so 40 of 1000; age >= 105
    // additionally keeps i%100 == 95, so 50. And nothing exceeds 109.
    cmd()
        .args([&table.path(), "-w", "age > 105"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 40"));
    cmd()
        .args([&table.path(), "-w", "age >= 105"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 50"));
    cmd()
        .args([&table.path(), "-w", "age > 109"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 0"));
}

#[test]
fn assert_stats_passes_when_every_file_carries_stats() {
    let table = thousand_file_table();
    cmd()
        .args([&table.path(), "--assert-stats"])
        .assert()
        .success();
}

#[test]
fn stats_mode_partial_on_a_synthesized_gap() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .add_file("f1.parquet", &[], None)
        .build();
    let output = cmd()
        .args([&table.path(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats"]["mode"], "partial");
    assert_eq!(json["stats"]["files_with_stats"], 1);
}

// ── Negative log shapes ─────────────────────────────────────────────

#[test]
fn malformed_stats_blob_counts_as_missing_statistics() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .add_file(
            "f1.parquet",
            &[],
            Some(serde_json::json!("not a stats object")),
        )
        .build();
    let output = cmd()
        .args([&table.path(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats"]["mode"], "partial");
    assert_eq!(json["stats"]["files_with_stats"], 1);

    cmd()
        .args([&table.path(), "--assert-stats"])
        .assert()
        .failure();
}

#[test]
fn directory_without_a_delta_log_fails_cleanly() {
    let dir = tempfile::TempDir::new().unwrap();
    cmd()
        .arg(dir.path().to_string_lossy().as_ref())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn empty_delta_log_directory_fails_cleanly() {
    let dir = tempfile::TempDir::new().unwrap();
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    cmd()
        .arg(dir.path().to_string_lossy().as_ref())
        .assert()
        .failure()
        .stderr(predicate::str::contains("No files in log segment"));
}
