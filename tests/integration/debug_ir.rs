//! --debug-ir: the diagnostic dump of the run's intermediate
//! representations. The dump format is explicitly unstable, so these tests
//! pin the section inventory and the load-bearing semantics (stripping,
//! normalization visibility), not the exact rendering.

use crate::common::{cmd, fixture};

fn run_dump(table: &str, predicate: Option<&str>) -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ir.txt");
    let mut command = cmd();
    command.arg(fixture(table));
    if let Some(pred) = predicate {
        command.args(["-w", pred]);
    }
    command.arg("--debug-ir").arg(&path).assert().success();
    std::fs::read_to_string(&path).unwrap()
}

/// The body of one `== title ==` section, up to the next section header.
fn section<'a>(dump: &'a str, title: &str) -> &'a str {
    let header = format!("== {title} ==");
    let start = dump
        .find(&header)
        .unwrap_or_else(|| panic!("missing section '{title}' in dump:\n{dump}"))
        + header.len();
    let rest = &dump[start..];
    match rest.find("\n== ") {
        Some(end) => &rest[..end],
        None => rest,
    }
}

#[test]
fn mixed_predicate_dumps_every_pipeline_section() {
    let dump = run_dump("test-table", Some("country = 'DE' AND age > 40"));
    for title in [
        "invocation",
        "snapshot",
        "owned AST (parsed)",
        "owned AST (normalized)",
        "classification",
        "kernel predicate: partition-only scan",
        "kernel predicate: full scan",
        "survivor sets",
        "kernel trace",
    ] {
        section(&dump, title);
    }
    assert!(dump.contains("delta-explain debug IR dump"));
}

#[test]
fn full_scan_predicate_is_stripped_of_unsupported_fragments() {
    let dump = run_dump("test-table", Some("country LIKE 'D%' AND age > 40"));
    let full = section(&dump, "kernel predicate: full scan");
    assert!(
        full.contains("age"),
        "stripped predicate lost the supported fragment:\n{full}"
    );
    assert!(
        !full.contains("LIKE"),
        "unsupported fragment leaked into the scan predicate:\n{full}"
    );
    // The fragment is still visible upstream, where it belongs.
    assert!(section(&dump, "classification").contains("LIKE"));
}

#[test]
fn normalization_rewrites_are_visible_between_the_two_ast_sections() {
    let dump = run_dump("test-table", Some("NOT (country != 'DE' OR age <= 40)"));
    let normalized = section(&dump, "owned AST (normalized)");
    assert!(
        normalized.contains("country = 'DE'"),
        "De Morgan rewrite not visible:\n{normalized}"
    );
    assert!(
        normalized.contains("age > 40"),
        "comparison flip not visible:\n{normalized}"
    );
}

#[test]
fn run_without_predicate_still_produces_header_snapshot_and_trace() {
    let dump = run_dump("test-table", None);
    assert!(dump.contains("delta-explain debug IR dump"));
    assert!(section(&dump, "invocation").contains("(none)"));
    section(&dump, "snapshot");
    section(&dump, "kernel trace");
}

#[test]
fn pure_partition_predicate_reports_skipped_full_scan() {
    let dump = run_dump("test-table", Some("country = 'DE'"));
    let survivors = section(&dump, "survivor sets");
    assert!(survivors.contains("full scan: skipped (pure-partition predicate)"));
}
