//! The JSON output validated against the published schema file: a consumer
//! can trust `schemas/report-v0.5.schema.json` because every document the
//! binary emits in this suite must satisfy it. The schema is strict
//! (additionalProperties: false), so an accidental new field fails here
//! before it silently becomes contract.

use crate::common::{LogBuilder, cmd, fixture, int_range_stats};

fn schema() -> jsonschema::Validator {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let raw = std::fs::read_to_string(format!("{manifest_dir}/schemas/report-v0.5.schema.json"))
        .expect("schema file");
    let schema_json: serde_json::Value = serde_json::from_str(&raw).expect("schema parses");
    jsonschema::validator_for(&schema_json).expect("schema compiles")
}

fn assert_valid(validator: &jsonschema::Validator, args: &[&str]) -> serde_json::Value {
    let output = cmd().args(args).output().unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON for {args:?}: {e}"));
    let errors: Vec<String> = validator
        .iter_errors(&doc)
        .map(|e| format!("{} at {}", e, e.instance_path()))
        .collect();
    assert!(
        errors.is_empty(),
        "schema violations for {args:?}:\n{}",
        errors.join("\n")
    );

    doc
}

#[test]
fn every_document_shape_satisfies_the_published_schema() {
    let validator = schema();
    let table = fixture("test-table");

    // no predicate
    assert_valid(&validator, &[&table, "--format", "json"]);
    // predicate, two phases
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
        ],
    );
    // unsplittable + incomplete confidence
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE' OR age > 30",
            "--format",
            "json",
        ],
    );
    // degraded unsupported fragment (UNSUPPORTED_EXPRESSION note)
    assert_valid(
        &validator,
        &[&table, "-w", "UPPER(name) = 'X'", "--format", "json"],
    );
    // gates, passing and failing
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE'",
            "--min-pruning",
            "50",
            "--assert-stats",
            "--format",
            "json",
        ],
    );
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE'",
            "--min-pruning",
            "99",
            "--format",
            "json",
        ],
    );
    // verbose per-file detail, with and without truncation
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE'",
            "--format",
            "json",
            "--verbose",
        ],
    );
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE'",
            "--format",
            "json",
            "--verbose",
            "--limit",
            "2",
        ],
    );
    // --explain-why: the additive `explain` array (with and without
    // diagnoses) must satisfy the schema.
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "UPPER(name) = 'X'",
            "--explain-why",
            "--format",
            "json",
        ],
    );
    assert_valid(
        &validator,
        &[
            &table,
            "-w",
            "country = 'DE'",
            "--explain-why",
            "--format",
            "json",
        ],
    );
}

#[test]
fn feature_bearing_tables_satisfy_the_published_schema() {
    let validator = schema();

    let dv_table = LogBuilder::new()
        .column("age", "integer")
        .property("delta.enableDeletionVectors", "true")
        .reader_feature("deletionVectors")
        .writer_feature("deletionVectors")
        .add_file_with_dv("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .add_file("f1.parquet", &[], None) // stats.mode = partial
        .build();
    assert_valid(&validator, &[&dv_table.path(), "--format", "json"]);
    assert_valid(
        &validator,
        &[&dv_table.path(), "--format", "json", "--verbose"],
    );

    let clustered = LogBuilder::new()
        .column("age", "integer")
        .domain_metadata("delta.clustering", "{\"clusteringColumns\":[[\"age\"]]}")
        .add_file("f0.parquet", &[], Some(int_range_stats("age", 0, 10, 100)))
        .build();
    assert_valid(&validator, &[&clustered.path(), "--format", "json"]);

    let mapped = LogBuilder::new()
        .mapped_column("age", "integer", 1, "col-a1")
        .property("delta.columnMapping.mode", "name")
        .reader_feature("columnMapping")
        .writer_feature("columnMapping")
        .add_file(
            "f0.parquet",
            &[],
            Some(int_range_stats("col-a1", 0, 10, 100)),
        )
        .build();
    assert_valid(&validator, &[&mapped.path(), "--format", "json"]);
}

#[test]
fn mapped_stats_coverage_uses_logical_name_and_all_candidate_files() {
    // Arrange
    let validator = schema();

    let table = LogBuilder::new()
        .mapped_column("age", "integer", 1, "col-a1")
        .property("delta.columnMapping.mode", "name")
        .reader_feature("columnMapping")
        .writer_feature("columnMapping")
        .add_file(
            "f0.parquet",
            &[],
            Some(int_range_stats("col-a1", 0, 10, 100)),
        )
        .add_file("f1.parquet", &[], None)
        .build();

    // Act
    let table_path = table.path();

    let doc = assert_valid(
        &validator,
        &[&table_path, "-w", "age > 5", "--format", "json"],
    );

    // Assert
    let coverage = doc
        .pointer("/analysis/stats_coverage")
        .and_then(serde_json::Value::as_array)
        .expect("analysis.stats_coverage should be an array");

    assert_eq!(coverage.len(), 1);

    let age = &coverage[0];

    assert_eq!(age["column"].as_str(), Some("age"));
    assert_eq!(age["requirement"].as_str(), Some("min_max"));
    assert_eq!(age["candidate_files"].as_u64(), Some(2));
    assert_eq!(age["covered_files"].as_u64(), Some(1));
    assert_eq!(age["coverage_pct"].as_f64(), Some(50.0));
}
