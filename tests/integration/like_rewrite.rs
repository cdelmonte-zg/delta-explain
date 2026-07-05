//! Prefix LIKE rewriting (issue #72): normalization turns `col LIKE 'p%'`
//! into a lexicographic range and a wildcard-free pattern into equality,
//! so the fragment prunes through the ordinary column rules on both the
//! partition and the stats axis. Every other LIKE shape keeps the
//! conservative degradation path.

use crate::common::{LogBuilder, cmd, fixture};
use predicates::prelude::*;

fn str_range_stats(column: &str, min: &str, max: &str) -> serde_json::Value {
    serde_json::json!({
        "numRecords": 10,
        "minValues": { column: min },
        "maxValues": { column: max },
        "nullCount": { column: 0 },
    })
}

// ── Partition axis: the rewritten range compares partition values ───

#[test]
fn prefix_like_on_a_partition_column_prunes_exactly() {
    cmd()
        .args([&fixture("test-table"), "-w", "country LIKE 'D%'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country >= 'D' AND country < 'E'")
                .and(predicate::str::contains("files remaining: 2"))
                .and(predicate::str::contains("confidence:     exact"))
                .and(predicate::str::contains("Warnings!").not()),
        );
}

#[test]
fn wildcard_free_like_behaves_as_equality() {
    cmd()
        .args([&fixture("test-table"), "-w", "country LIKE 'DE'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country = 'DE'")
                .and(predicate::str::contains("files remaining: 2"))
                .and(predicate::str::contains("confidence:     exact")),
        );
}

// ── Stats axis: the rewritten range skips on string min/max ─────────

#[test]
fn prefix_like_on_a_data_column_skips_on_min_max() {
    let table = LogBuilder::new()
        .column("name", "string")
        .add_file(
            "a.parquet",
            &[],
            Some(str_range_stats("name", "Alice", "Bob")),
        )
        .add_file(
            "b.parquet",
            &[],
            Some(str_range_stats("name", "Carl", "Dave")),
        )
        .add_file(
            "c.parquet",
            &[],
            Some(str_range_stats("name", "Xavier", "Zoe")),
        )
        .build();

    cmd()
        .args([&table.path(), "-w", "name LIKE 'A%'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("stats-safe:     name >= 'A' AND name < 'B'")
                .and(predicate::str::contains("files remaining: 1"))
                .and(predicate::str::contains("confidence:     conservative")),
        );
}

// ── Non-prefix shapes on a data column keep the degradation path ────
// (on a partition column they are evaluated exactly instead: see the
// partition_exact module)

#[test]
fn non_prefix_like_on_a_data_column_degrades_conservatively() {
    cmd()
        .args([&fixture("test-table"), "-w", "name LIKE '%son'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 6")
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION"))
                .and(predicate::str::contains("confidence:     incomplete")),
        );
}

#[test]
fn not_like_on_a_data_column_degrades_conservatively() {
    cmd()
        .args([&fixture("test-table"), "-w", "name NOT LIKE 'D%'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 6")
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION"))
                .and(predicate::str::contains("confidence:     incomplete")),
        );
}

#[test]
fn non_prefix_like_under_and_does_not_poison_siblings() {
    cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country LIKE 'D%' AND name LIKE '%son'",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country >= 'D' AND country < 'E'")
                .and(predicate::str::contains("unsplittable:   name LIKE '%son'"))
                .and(predicate::str::contains("files remaining: 2")),
        );
}
