//! Exact evaluation of partition-only fragments against the literal
//! partition values (issue #75): constructs outside the kernel's language
//! whose semantics are known - LIKE in any shape - prune exactly on the
//! partition axis instead of degrading. Opaque constructs (functions,
//! arithmetic, subqueries) still degrade, partition column or not.

use crate::common::{LogBuilder, cmd, fixture};
use predicates::prelude::*;

#[test]
fn non_prefix_like_on_a_partition_column_prunes_exactly() {
    // '%E' matches only DE among DE/US/IT; no warning, exact confidence.
    cmd()
        .args([&fixture("test-table"), "-w", "country LIKE '%E'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-exact: country LIKE '%E'")
                .and(predicate::str::contains(
                    "Phase 1: Partition pruning [exact]",
                ))
                .and(predicate::str::contains("files remaining: 2"))
                .and(predicate::str::contains("confidence:     exact"))
                .and(predicate::str::contains("Warnings!").not()),
        );
}

#[test]
fn not_like_on_a_partition_column_prunes_exactly() {
    cmd()
        .args([&fixture("test-table"), "-w", "country NOT LIKE 'D%'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-exact: country NOT LIKE 'D%'")
                .and(predicate::str::contains("files remaining: 4"))
                .and(predicate::str::contains("confidence:     exact")),
        );
}

#[test]
fn exact_fragment_intersects_with_the_kernel_partition_scan() {
    // IN lowers to the kernel, LIKE evaluates locally; phase 1 is the
    // intersection and both fragments appear on its predicate line.
    cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country IN ('DE', 'US') AND country LIKE '%E'",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country IN ('DE', 'US')")
                .and(predicate::str::contains(
                    "partition-exact: country LIKE '%E'",
                ))
                .and(predicate::str::contains("files remaining: 2")),
        );
}

#[test]
fn exact_fragment_chains_with_data_skipping() {
    // The final scan cannot honor the exact fragment (the kernel has no
    // LIKE), so phase 2 must still chain from phase 1's survivors: the
    // same files as `country = 'DE' AND age > 40`, never more.
    cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country LIKE '%E' AND age > 40",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-exact: country LIKE '%E'")
                .and(predicate::str::contains("stats-safe:     age > 40"))
                .and(predicate::str::contains(
                    "Phase 1: Partition pruning [exact]",
                ))
                .and(predicate::str::contains(
                    "Phase 2: Data skipping (min/max statistics)",
                ))
                .and(predicate::str::contains("Total reduction: 6 -> 1 files"))
                .and(predicate::str::contains("confidence:     conservative")),
        );
}

#[test]
fn null_partition_value_is_dropped_exactly_not_kept() {
    // The fragment is constant per file and NULL selects no row: the
    // null-country file must be pruned, exactly as an engine would.
    let table = LogBuilder::new()
        .column("age", "integer")
        .partition_column("country", "string")
        .add_file_nullable("de.parquet", &[("country", Some("DE"))], None)
        .add_file_nullable("null.parquet", &[("country", None)], None)
        .build();

    cmd()
        .args([&table.path(), "-w", "country LIKE '%E'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 1")
                .and(predicate::str::contains("confidence:     exact")),
        );

    // ...and IS NULL, two-valued, keeps exactly the null file.
    cmd()
        .args([&table.path(), "-w", "country IS NULL"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 1"));
}

#[test]
fn opaque_constructs_still_degrade_even_on_partition_columns() {
    cmd()
        .args([&fixture("test-table"), "-w", "UPPER(country) = 'DE'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 6")
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION"))
                .and(predicate::str::contains("confidence:     incomplete")),
        );
}

#[test]
fn mixed_like_across_partition_and_data_columns_stays_unsplittable() {
    // The OR spans a partition and a data column: not separable, and the
    // exact route requires every column to be a partition column.
    cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country LIKE '%E' OR name LIKE '%son'",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 6")
                .and(predicate::str::contains("confidence:     incomplete")),
        );
}

#[test]
fn partition_exact_appears_in_json_analysis() {
    let output = cmd()
        .args([
            &fixture("test-table"),
            "-w",
            "country LIKE '%E'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    let analysis = &json["analysis"];
    assert_eq!(analysis["partition_exact"], "country LIKE '%E'");
    assert!(analysis["partition_safe"].is_null());
    assert!(analysis["unsplittable"].is_null());
    assert_eq!(analysis["confidence"], "exact");
    assert_eq!(json["final_files"], 2);
}
