//! Detect-and-declare: protocol features that distort or reframe the
//! numbers (deletion vectors, column mapping, liquid clustering) are
//! declared in the JSON `table_features` block and warned about in both
//! formats. Detection never changes how pruning is computed.

use predicates::prelude::*;

use crate::common::{LogBuilder, cmd, fixture, int_range_stats};

fn run_json(args: &[&str]) -> serde_json::Value {
    let output = cmd().args(args).output().unwrap();
    assert!(output.status.success(), "stderr: {:?}", output.stderr);
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn plain_fixture_declares_no_features() {
    let json = run_json(&[&fixture("test-table"), "--format", "json"]);
    let tf = &json["table_features"];
    assert_eq!(tf["deletion_vectors"]["enabled"], false);
    assert_eq!(tf["deletion_vectors"]["files_with_deletion_vectors"], 0);
    assert!(tf["column_mapping_mode"].is_null());
    assert!(tf["clustering_columns"].is_null());
    assert_eq!(tf["notes"].as_array().unwrap().len(), 0);
}

#[test]
fn files_with_deletion_vectors_warn_with_counts() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .property("delta.enableDeletionVectors", "true")
        .reader_feature("deletionVectors")
        .writer_feature("deletionVectors")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .add_file_with_dv("f1.parquet", &[], Some(int_range_stats("age", 5, 20, 100)))
        .build();

    let json = run_json(&[&table.path(), "--format", "json"]);
    let tf = &json["table_features"];
    assert_eq!(tf["deletion_vectors"]["enabled"], true);
    assert_eq!(tf["deletion_vectors"]["files_with_deletion_vectors"], 1);
    assert_eq!(tf["notes"][0]["code"], "DELETION_VECTORS");

    cmd().arg(table.path()).assert().success().stdout(
        predicate::str::contains("Warnings!")
            .and(predicate::str::contains("DELETION_VECTORS"))
            .and(predicate::str::contains("1 of 2 files")),
    );
}

#[test]
fn enabled_but_unused_deletion_vectors_declare_without_warning() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .property("delta.enableDeletionVectors", "true")
        .reader_feature("deletionVectors")
        .writer_feature("deletionVectors")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .build();

    let json = run_json(&[&table.path(), "--format", "json"]);
    let tf = &json["table_features"];
    assert_eq!(tf["deletion_vectors"]["enabled"], true);
    assert_eq!(tf["deletion_vectors"]["files_with_deletion_vectors"], 0);
    assert_eq!(tf["notes"].as_array().unwrap().len(), 0);

    cmd()
        .arg(table.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("Warnings!").not());
}

#[test]
fn pruning_still_works_alongside_deletion_vectors() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .reader_feature("deletionVectors")
        .writer_feature("deletionVectors")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .add_file_with_dv("f1.parquet", &[], Some(int_range_stats("age", 50, 60, 100)))
        .build();

    cmd()
        .args([&table.path(), "-w", "age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 1")
                .and(predicate::str::contains("DELETION_VECTORS")),
        );
}

#[test]
fn column_mapping_by_name_declares_and_warns() {
    // Under name mapping the log stores physical names: partitionValues and
    // stats keys use them, which is exactly the display gap the warning
    // declares.
    let table = LogBuilder::new()
        .mapped_column("age", "integer", 1, "col-a1")
        .property("delta.columnMapping.mode", "name")
        .property("delta.columnMapping.maxColumnId", "1")
        .reader_feature("columnMapping")
        .writer_feature("columnMapping")
        .add_file(
            "f0.parquet",
            &[],
            Some(int_range_stats("col-a1", 0, 10, 100)),
        )
        .build();

    let json = run_json(&[&table.path(), "--format", "json"]);
    let tf = &json["table_features"];
    assert_eq!(tf["column_mapping_mode"], "name");
    assert_eq!(tf["notes"][0]["code"], "COLUMN_MAPPING");
}

#[test]
fn liquid_clustering_declares_the_clustering_columns() {
    let table = LogBuilder::new()
        .column("age", "integer")
        .column("score", "double")
        .writer_feature("domainMetadata")
        .writer_feature("clustering")
        .domain_metadata(
            "delta.clustering",
            "{\"clusteringColumns\":[[\"age\"],[\"score\"]]}",
        )
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .build();

    let json = run_json(&[&table.path(), "--format", "json"]);
    let tf = &json["table_features"];
    assert_eq!(tf["clustering_columns"][0], "age");
    assert_eq!(tf["clustering_columns"][1], "score");
    assert_eq!(tf["notes"][0]["code"], "LIQUID_CLUSTERING");

    cmd().arg(table.path()).assert().success().stdout(
        predicate::str::contains("LIQUID_CLUSTERING").and(predicate::str::contains("age, score")),
    );
}

#[test]
fn table_warnings_and_analysis_warnings_share_the_text_section() {
    // A DV table queried with an unsupported fragment shows both warning
    // kinds under one "Warnings!" header.
    let table = LogBuilder::new()
        .column("age", "integer")
        .reader_feature("deletionVectors")
        .writer_feature("deletionVectors")
        .add_file_with_dv("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .build();

    cmd()
        .args([&table.path(), "-w", "UPPER(name) = 'X'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("DELETION_VECTORS")
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION")),
        );
}
