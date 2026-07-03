//! Semantic regression suite — Step 1.5.
//!
//! One test per "minimum case" listed in DELTA-EXPLAIN-ROADMAP.md (Step 1.5).
//! Each test is the canonical, end-to-end check for that scenario: input
//! predicate + fixture in, JSON output asserted against. If you change the
//! pipeline and break the contract for one of these cases, this file is the
//! single place that should fail.
//!
//! All assertions are made against the stable JSON schema (v0.1.0) — text
//! output is the *human* surface, JSON is the *machine* surface, and that's
//! what we lock down here.

use crate::common::{cmd, fixture};
use serde_json::Value;

/// Run delta-explain with the given args and parse stdout as JSON.
fn run_json(table: &str, predicate: Option<&str>) -> Value {
    let mut args: Vec<String> = vec![table.into(), "--format".into(), "json".into()];
    if let Some(p) = predicate {
        args.push("-w".into());
        args.push(p.into());
    }
    let output = cmd().args(&args).output().unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}

// ── Case 1: predicate only on partition column → phase 1 only ──────

/// Roadmap case 1: a predicate that touches only a partition column must
/// resolve via partition pruning alone, with `confidence == "exact"` because
/// no stats-based reasoning is involved.
#[test]
fn case_1_partition_only_predicate() {
    let json = run_json(&fixture("test-table"), Some("country = 'DE'"));

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 1, "partition-only predicate uses one phase");
    assert_eq!(phases[0]["name"], "Partition pruning");

    let analysis = &json["analysis"];
    assert_eq!(analysis["partition_safe"], "country = 'DE'");
    assert!(analysis["stats_safe"].is_null());
    assert!(analysis["unsplittable"].is_null());
    assert_eq!(analysis["confidence"], "exact");
    assert!(analysis["notes"].as_array().unwrap().is_empty());

    assert_eq!(json["final_files"], 2);
}

// ── Case 2: predicate only on data column → phase 2 only ───────────

/// Roadmap case 2: a predicate that touches only a non-partition column
/// must resolve via data skipping alone, with `confidence == "conservative"`
/// because min/max stats can over-keep files.
#[test]
fn case_2_data_only_predicate() {
    let json = run_json(&fixture("test-table"), Some("age > 30"));

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 1, "data-only predicate uses one phase");
    assert_eq!(phases[0]["name"], "Data skipping (min/max statistics)");

    let analysis = &json["analysis"];
    assert!(analysis["partition_safe"].is_null());
    assert_eq!(analysis["stats_safe"], "age > 30");
    assert!(analysis["unsplittable"].is_null());
    assert_eq!(analysis["confidence"], "conservative");
    assert!(analysis["notes"].as_array().unwrap().is_empty());

    assert_eq!(json["final_files"], 5);
}

// ── Case 3: mixed AND → both phases ─────────────────────────────────

/// Roadmap case 3: an AND predicate spanning a partition and a non-partition
/// column must split into two distinct phases. Confidence is `conservative`
/// because the second phase relies on stats.
#[test]
fn case_3_mixed_and_predicate() {
    let json = run_json(&fixture("test-table"), Some("country = 'DE' AND age > 40"));

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 2, "mixed AND predicate uses two phases");
    assert_eq!(phases[0]["name"], "Partition pruning");
    assert_eq!(phases[0]["confidence"], "exact");
    assert_eq!(phases[1]["name"], "Data skipping (min/max statistics)");
    assert_eq!(phases[1]["confidence"], "conservative");

    let analysis = &json["analysis"];
    assert_eq!(analysis["partition_safe"], "country = 'DE'");
    assert_eq!(analysis["stats_safe"], "age > 40");
    assert!(analysis["unsplittable"].is_null());
    assert_eq!(analysis["confidence"], "conservative");

    assert_eq!(json["final_files"], 1);
}

// ── Case 4: OR not separable → phase 2 with note + incomplete ──────

/// Roadmap case 4: an OR that mixes partition and non-partition columns
/// cannot be split safely; the whole predicate is routed to data skipping
/// with `confidence == "incomplete"` and an `UNSPLITTABLE_OR` note.
#[test]
fn case_4_unsplittable_or_predicate() {
    let json = run_json(&fixture("test-table"), Some("country = 'DE' OR age > 30"));

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 1, "unsplittable OR collapses to one phase");
    assert_eq!(phases[0]["name"], "Data skipping (min/max statistics)");
    assert_eq!(phases[0]["confidence"], "incomplete");

    let analysis = &json["analysis"];
    assert!(analysis["partition_safe"].is_null());
    assert!(analysis["stats_safe"].is_null());
    assert_eq!(analysis["unsplittable"], "country = 'DE' OR age > 30");
    assert_eq!(analysis["confidence"], "incomplete");

    let notes = analysis["notes"].as_array().unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0]["code"], "UNSPLITTABLE_OR");
}

// ── Case 5: stats missing → confidence stays conservative ──────────

/// Roadmap case 5: when some files lack log-level statistics, the analyzer
/// still labels the predicate `conservative` (not `exact`). Files without
/// stats are kept by the kernel since their min/max cannot be inspected,
/// so the final count reflects that conservative behavior.
#[test]
fn case_5_partial_stats_keeps_confidence_conservative() {
    let json = run_json(
        &fixture("test-table-partial-stats"),
        Some("country = 'DE' AND age > 30"),
    );

    assert_eq!(json["stats"]["mode"], "partial");
    assert_eq!(json["stats"]["files_with_stats"], 2);
    assert_eq!(json["stats"]["total_files"], 4);

    let analysis = &json["analysis"];
    assert_eq!(
        analysis["confidence"], "conservative",
        "predicate with stats_safe fragment must report conservative even when stats are partial"
    );

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 2);
    // Of the 2 DE files, one has stats (age 45-52, kept) and one doesn't
    // (kernel keeps it because it cannot prove exclusion).
    assert_eq!(json["final_files"], 2);
}

// ── Case 6: empty table → 0 files, output remains correct ──────────

/// Roadmap case 6: an empty table (zero Add actions, metadata only) must
/// not crash the tool, must report zero files at every stage, and must
/// still emit a well-formed JSON document including the predicate analysis.
#[test]
fn case_6_empty_table() {
    let json = run_json(&fixture("test-table-empty"), Some("region = 'EU'"));

    assert_eq!(json["total_files"], 0);
    assert_eq!(json["final_files"], 0);
    assert_eq!(json["stats"]["mode"], "absent");
    assert_eq!(json["stats"]["total_files"], 0);

    let analysis = &json["analysis"];
    assert_eq!(analysis["partition_safe"], "region = 'EU'");
    assert_eq!(analysis["confidence"], "exact");

    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 1);
    assert_eq!(phases[0]["input_files"], 0);
    assert_eq!(phases[0]["output_files"], 0);
}
