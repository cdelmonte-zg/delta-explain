use crate::common::{cmd, fixture};
use predicates::prelude::*;
use rstest::rstest;

fn test_table() -> String {
    fixture("test-table")
}

fn test_table_flat() -> String {
    fixture("test-table-flat")
}

// ── Basic snapshot ──────────────────────────────────────────────────

#[test]
fn no_predicate_shows_file_count() {
    cmd()
        .arg(test_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 6"));
}

#[test]
fn no_predicate_shows_version() {
    cmd()
        .arg(test_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Version:     5"));
}

// ── Partition pruning ───────────────────────────────────────────────

#[test]
fn partition_pruning_country_de() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning")
                .and(predicate::str::contains("files remaining: 2")),
        );
}

#[test]
fn partition_pruning_country_us() {
    cmd()
        .args([&test_table(), "-w", "country = 'US'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 2"));
}

#[test]
fn partition_pruning_country_it() {
    cmd()
        .args([&test_table(), "-w", "country = 'IT'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 2"));
}

// ── Data skipping only ──────────────────────────────────────────────

#[test]
fn data_skipping_age_gt_30() {
    cmd()
        .args([&test_table(), "-w", "age > 30"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Data skipping")
                .and(predicate::str::contains("files remaining: 5")),
        );
}

#[test]
fn data_skipping_age_gt_60() {
    cmd()
        .args([&test_table(), "-w", "age > 60"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 1"));
}

// ── Combined: partition + data skipping ─────────────────────────────

#[test]
fn combined_country_de_age_gt_40() {
    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning")
                .and(predicate::str::contains("Phase 2: Data skipping"))
                .and(predicate::str::contains("Total reduction: 6 -> 1 files")),
        );
}

#[test]
fn combined_preserves_phase_order() {
    let output = cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'"])
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);

    let phase1_pos = stdout.find("Phase 1: Partition pruning").unwrap();
    let phase2_pos = stdout.find("Phase 2: Data skipping").unwrap();
    assert!(phase1_pos < phase2_pos);
}

// ── Verbose output ──────────────────────────────────────────────────

#[test]
fn verbose_shows_kept_and_dropped() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--verbose"])
        .assert()
        .success()
        .stdout(predicate::str::contains("[KEPT   ]").and(predicate::str::contains("[DROPPED]")));
}

#[test]
fn verbose_shows_partition_values() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--verbose"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition(country=DE)")
                .and(predicate::str::contains("partition(country=IT)"))
                .and(predicate::str::contains("partition(country=US)")),
        );
}

#[test]
fn verbose_shows_stats() {
    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--verbose",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("stats(age:").and(predicate::str::contains("..")));
}

#[test]
fn verbose_shows_file_size() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--verbose"])
        .assert()
        .success()
        .stdout(predicate::str::contains("KB"));
}

// ── Reduction percentages ───────────────────────────────────────────

#[test]
fn shows_pruning_percentage() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("67% pruned"));
}

#[test]
fn combined_shows_total_reduction() {
    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("83% pruned"));
}

// ── JSON output ─────────────────────────────────────────────────────

#[test]
fn json_output_is_valid() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["total_files"], 6);
    assert_eq!(json["final_files"], 1);
    assert_eq!(json["version"], 5);
}

#[test]
fn json_contains_phases() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let phases = json["phases"].as_array().unwrap();
    assert_eq!(phases.len(), 2);
    assert_eq!(phases[0]["name"], "Partition pruning");
    assert_eq!(phases[1]["name"], "Data skipping (min/max statistics)");
}

#[test]
fn json_contains_stats_block() {
    let output = cmd()
        .args([&test_table(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats"]["files_with_stats"], 6);
    assert_eq!(json["stats"]["total_files"], 6);
    assert_eq!(json["stats"]["mode"], "exact");
}

// ── CI assertions ───────────────────────────────────────────────────

#[test]
fn min_pruning_passes_when_above_threshold() {
    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--min-pruning",
            "50",
        ])
        .assert()
        .success();
}

#[test]
fn min_pruning_fails_when_below_threshold() {
    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--min-pruning",
            "90",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ASSERTION FAILED"));
}

#[test]
fn min_pruning_exact_boundary() {
    // 83.33% pruning — threshold 83 should pass, 84 should fail
    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--min-pruning",
            "83",
        ])
        .assert()
        .success();

    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--min-pruning",
            "84",
        ])
        .assert()
        .failure();
}

#[test]
fn assert_stats_passes_when_all_present() {
    cmd()
        .args([&test_table(), "--assert-stats"])
        .assert()
        .success();
}

#[test]
fn assert_stats_combinable_with_predicate() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--assert-stats"])
        .assert()
        .success();
}

#[test]
fn json_and_min_pruning_combinable() {
    cmd()
        .args([
            &test_table(),
            "-w",
            "age > 40 AND country = 'DE'",
            "--format",
            "json",
            "--min-pruning",
            "50",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"total_pruning_pct\""));
}

// ── Edge cases ──────────────────────────────────────────────────────

#[test]
fn predicate_matching_all_files() {
    cmd()
        .args([&test_table(), "-w", "age > 0"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 6"));
}

#[test]
fn invalid_column_returns_error() {
    cmd()
        .args([&test_table(), "-w", "nonexistent > 5"])
        .assert()
        .failure();
}

#[test]
fn invalid_path_returns_error() {
    cmd().args(["./does-not-exist"]).assert().failure();
}

// ── SQL predicate features ──────────────────────────────────────────

#[test]
fn or_predicate() {
    // country = 'DE' OR country = 'US' -> should match 4 files (2 DE + 2 US)
    cmd()
        .args([&test_table(), "-w", "country = 'DE' OR country = 'US'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 4"));
}

#[test]
fn in_list_predicate() {
    // country IN ('DE', 'IT') -> should match 4 files
    cmd()
        .args([&test_table(), "-w", "country IN ('DE', 'IT')"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 4"));
}

#[test]
fn between_predicate() {
    // age BETWEEN 40 AND 60 -> data skipping should keep files with overlapping ranges
    cmd()
        .args([&test_table(), "-w", "age BETWEEN 40 AND 60"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Data skipping"));
}

#[test]
fn not_predicate() {
    // NOT country = 'DE' -> should drop DE files, keep US + IT = 4 files
    cmd()
        .args([&test_table(), "-w", "NOT country = 'DE'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 4"));
}

#[test]
fn is_not_null_predicate() {
    // age IS NOT NULL -> all files have age, should keep all 6
    cmd()
        .args([&test_table(), "-w", "age IS NOT NULL"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 6"));
}

#[test]
fn parenthesized_predicate() {
    // (age > 40) AND (country = 'DE') -> same as without parens
    cmd()
        .args([&test_table(), "-w", "(age > 40) AND (country = 'DE')"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Total reduction: 6 -> 1 files"));
}

#[test]
fn complex_or_and_combination() {
    // (country = 'DE' OR country = 'IT') AND age > 40
    cmd()
        .args([
            &test_table(),
            "-w",
            "(country = 'DE' OR country = 'IT') AND age > 40",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining:"));
}

#[test]
fn not_in_predicate() {
    // country NOT IN ('US') -> should keep DE + IT = 4 files
    cmd()
        .args([&test_table(), "-w", "country NOT IN ('US')"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 4"));
}

// ── SQL predicate edge cases ────────────────────────────────────────

#[test]
fn in_single_element_same_as_eq() {
    // IN ('DE') should behave like = 'DE'
    let in_output = cmd()
        .args([&test_table(), "-w", "country IN ('DE')", "--format", "json"])
        .output()
        .unwrap();
    let eq_output = cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--format", "json"])
        .output()
        .unwrap();
    let in_json: serde_json::Value = serde_json::from_slice(&in_output.stdout).unwrap();
    let eq_json: serde_json::Value = serde_json::from_slice(&eq_output.stdout).unwrap();
    assert_eq!(in_json["final_files"], eq_json["final_files"]);
}

#[test]
fn predicate_eliminates_all_files() {
    // age > 1000 -> no file has max(age) > 1000, all dropped
    cmd()
        .args([&test_table(), "-w", "age > 1000"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 0"));
}

#[test]
fn negative_literal() {
    // age > -10 -> all files have age > -10, keep all
    cmd()
        .args([&test_table(), "-w", "age > -10"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 6"));
}

#[test]
fn float_literal_in_predicate() {
    // score > 90.5 -> data skipping on float column
    cmd()
        .args([&test_table(), "-w", "score > 90.5"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Data skipping"));
}

#[test]
fn string_with_spaces() {
    // country = 'New Zealand' -> no match, but should parse correctly
    cmd()
        .args([&test_table(), "-w", "country = 'New Zealand'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 0"));
}

#[test]
fn between_on_partition_column() {
    // This is a mixed predicate: BETWEEN uses >= and <=, both on partition col
    // Not really meaningful for string partitions, but should not crash
    cmd()
        .args([&test_table(), "-w", "country BETWEEN 'A' AND 'F'"])
        .assert()
        .success();
}

#[test]
fn not_between() {
    cmd()
        .args([&test_table(), "-w", "age NOT BETWEEN 100 AND 200"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 6"));
}

#[test]
fn deeply_nested_parens() {
    cmd()
        .args([&test_table(), "-w", "((((age > 40)))) AND (country = 'DE')"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Total reduction: 6 -> 1 files"));
}

#[test]
fn double_or_and_combination() {
    // (a OR b) AND (c OR d)
    cmd()
        .args([
            &test_table(),
            "-w",
            "(country = 'DE' OR country = 'IT') AND (age > 30 OR score > 90)",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining:"));
}

#[test]
fn or_across_partition_and_stats() {
    // OR mixing partition and non-partition columns -> treated as stats predicate
    // because kernel can't split it
    cmd()
        .args([&test_table(), "-w", "country = 'DE' OR age > 50"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining:"));
}

#[test]
fn is_null_on_partition() {
    cmd()
        .args([&test_table(), "-w", "country IS NULL"])
        .assert()
        .success();
}

// ── Parse error handling ────────────────────────────────────────────

#[test]
fn invalid_sql_syntax() {
    cmd()
        .args([&test_table(), "-w", "age >>> 30"])
        .assert()
        .failure();
}

#[test]
fn empty_predicate() {
    cmd().args([&test_table(), "-w", ""]).assert().failure();
}

// ── Unsupported expressions degrade with a diagnostic, not an error ──

#[rstest]
#[case("UPPER(country) = 'DE'")]
#[case("age IN (SELECT 1)")]
#[case("name LIKE '%Hans%'")]
#[case("price * 2 > 100")]
fn unsupported_predicate_keeps_all_files_with_warning(#[case] predicate: &str) {
    cmd()
        .args([&test_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 6")
                .and(predicate::str::contains("Warnings!"))
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION"))
                .and(predicate::str::contains("confidence:     incomplete")),
        );
}

#[test]
fn unsupported_fragment_under_and_still_prunes_on_siblings() {
    // country = 'DE' keeps pruning to 2 files; the function fragment
    // degrades to keep-all instead of failing the whole command.
    cmd()
        .args([&test_table(), "-w", "country = 'DE' AND UPPER(name) = 'X'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country = 'DE'")
                .and(predicate::str::contains("files remaining: 2"))
                .and(predicate::str::contains("UNSUPPORTED_EXPRESSION")),
        );
}

#[test]
fn unsupported_inside_or_poisons_the_whole_or_conservatively() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE' OR UPPER(name) = 'X'",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["analysis"]["confidence"], "incomplete");
    assert_eq!(
        json["analysis"]["notes"][0]["code"],
        "UNSUPPORTED_EXPRESSION"
    );
    // conservative: the poisoned OR keeps every file
    assert_eq!(json["final_files"], 6);
}

// ── Null-safe comparison (IS DISTINCT FROM) ─────────────────────────

#[test]
fn is_distinct_from_prunes_partitions_exactly() {
    // Null-safe inequality on the partition column: DE files drop, US + IT stay.
    cmd()
        .args([&test_table(), "-w", "country IS DISTINCT FROM 'DE'"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country IS DISTINCT FROM 'DE'")
                .and(predicate::str::contains("files remaining: 4"))
                .and(predicate::str::contains("confidence:     exact")),
        );
}

#[test]
fn is_not_distinct_from_behaves_like_null_safe_equality() {
    cmd()
        .args([&test_table(), "-w", "country IS NOT DISTINCT FROM 'DE'"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 2"));
}

#[rstest]
#[case("country IS DISTINCT FROM NULL", 6)] // string partition col, = IS NOT NULL
#[case("age IS DISTINCT FROM NULL", 6)] // int stats col, all non-null
#[case("age IS NOT DISTINCT FROM NULL", 0)] // = IS NULL: proves real evaluation,
// a conservative keep-all would leave 6
fn distinct_from_null_evaluates_with_typed_null(#[case] predicate: &str, #[case] remaining: u32) {
    cmd()
        .args([&test_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "files remaining: {remaining}"
        )));
}

// ── Normalization rewrites (classification, not survivor sets) ──────

#[test]
fn not_over_mixed_or_splits_into_two_phases_by_de_morgan() {
    // NOT (country = 'DE' OR age > 30) normalizes to
    // country <> 'DE' AND age <= 30: partition-safe + stats-safe,
    // where the raw form would have been one unsplittable fragment.
    cmd()
        .args([&test_table(), "-w", "NOT (country = 'DE' OR age > 30)"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("partition-safe: country <> 'DE'")
                .and(predicate::str::contains("stats-safe:     age <= 30"))
                .and(predicate::str::contains("confidence:     conservative")),
        );
}

#[test]
fn or_factoring_recovers_the_common_partition_conjunct() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "(country = 'DE' AND age > 30) OR (country = 'DE' AND score > 90)",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["analysis"]["partition_safe"], "country = 'DE'");
    assert_eq!(json["analysis"]["stats_safe"], "age > 30 OR score > 90");
    assert!(json["analysis"]["unsplittable"].is_null());
    assert_eq!(json["analysis"]["confidence"], "conservative");
}

#[test]
fn or_absorption_collapses_to_the_partition_filter() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE' OR (country = 'DE' AND age > 30)",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["analysis"]["partition_safe"], "country = 'DE'");
    assert_eq!(json["analysis"]["confidence"], "exact");
    assert_eq!(json["final_files"], 2);
}

// ── Flat table (no partitions) ─────────────────────────────────────
// These tests use a table with no partition columns and mixed country
// values per file, demonstrating how pruning degrades without
// proper partitioning.

#[test]
fn flat_table_snapshot() {
    cmd()
        .arg(test_table_flat())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 6"));
}

#[test]
fn flat_no_partition_pruning_phase() {
    // With no partition columns, "Partition pruning" phase should not appear
    cmd()
        .args([&test_table_flat(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Partition pruning")
                .not()
                .and(predicate::str::contains(
                    "Data skipping (min/max statistics)",
                )),
        );
}

#[test]
fn flat_combined_predicate_keeps_4_files() {
    // Without partitioning, country min/max ranges are wide, so data skipping
    // can only eliminate files where max(age) <= 40
    cmd()
        .args([&test_table_flat(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("files remaining: 4")
                .and(predicate::str::contains("33% pruned")),
        );
}

#[test]
fn flat_vs_partitioned_pruning_contrast() {
    // Same predicate, same number of files — partitioned table prunes 83%,
    // flat table only 33%.
    let flat_out = cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let part_out = cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
        ])
        .output()
        .unwrap();

    let flat_json: serde_json::Value = serde_json::from_slice(&flat_out.stdout).unwrap();
    let part_json: serde_json::Value = serde_json::from_slice(&part_out.stdout).unwrap();

    // Both tables have 6 files
    assert_eq!(flat_json["total_files"], 6);
    assert_eq!(part_json["total_files"], 6);

    // Partitioned: 1 file survives (83% pruned)
    assert_eq!(part_json["final_files"], 1);
    // Flat: 4 files survive (33% pruned)
    assert_eq!(flat_json["final_files"], 4);

    let flat_pct = flat_json["total_pruning_pct"].as_f64().unwrap();
    let part_pct = part_json["total_pruning_pct"].as_f64().unwrap();
    assert!(
        part_pct > flat_pct,
        "partitioned ({part_pct}%) should prune more than flat ({flat_pct}%)"
    );
}

#[test]
fn flat_verbose_shows_dropped_files() {
    cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--verbose",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("[DROPPED] part-00001.snappy.parquet")
                .and(predicate::str::contains(
                    "[DROPPED] part-00002.snappy.parquet",
                ))
                .and(predicate::str::contains(
                    "[KEPT   ] part-00003.snappy.parquet",
                ))
                .and(predicate::str::contains(
                    "[KEPT   ] part-00004.snappy.parquet",
                ))
                .and(predicate::str::contains(
                    "[KEPT   ] part-00005.snappy.parquet",
                ))
                .and(predicate::str::contains(
                    "[KEPT   ] part-00006.snappy.parquet",
                )),
        );
}

#[test]
fn flat_no_total_reduction_with_single_phase() {
    // Only one phase → no "Total reduction" summary line
    cmd()
        .args([&test_table_flat(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Total reduction").not());
}

#[test]
fn flat_json_single_phase() {
    let output = cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let phases = json["phases"].as_array().unwrap();

    assert_eq!(phases.len(), 1);
    assert_eq!(phases[0]["name"], "Data skipping (min/max statistics)");
    assert_eq!(phases[0]["input_files"], 6);
    assert_eq!(phases[0]["output_files"], 4);
}

#[test]
fn flat_json_stats_block() {
    let output = cmd()
        .args([&test_table_flat(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(json["stats"]["files_with_stats"], 6);
    assert_eq!(json["stats"]["total_files"], 6);
    assert_eq!(json["stats"]["mode"], "exact");
}

// ── CI assertions (flat table) ─────────────────────────────────────

#[test]
fn flat_min_pruning_fails_at_90() {
    // Flat table achieves only 33% pruning — a 90% threshold must fail
    cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--min-pruning",
            "90",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "ASSERTION FAILED: total pruning 33.3% is below threshold 90.0%",
        ));
}

#[test]
fn flat_min_pruning_passes_at_30() {
    // 33% pruning is above a 30% threshold
    cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--min-pruning",
            "30",
        ])
        .assert()
        .success();
}

#[test]
fn flat_assert_stats_passes() {
    // All files in the flat table have statistics
    cmd()
        .args([&test_table_flat(), "--assert-stats"])
        .assert()
        .success();
}

#[test]
fn flat_min_pruning_with_json_format() {
    // CI mode: JSON output + assertion, both work together
    cmd()
        .args([
            &test_table_flat(),
            "-w",
            "country = 'DE' AND age > 40",
            "--format",
            "json",
            "--min-pruning",
            "90",
        ])
        .assert()
        .failure()
        .stdout(predicate::str::contains("\"total_pruning_pct\""))
        .stderr(predicate::str::contains("ASSERTION FAILED"));
}

// ── Predicate analysis output (Step 0.3) ────────────────────────────

/// Text output: global confidence label reflects the bucket assignment.
#[rstest]
#[case("country = 'DE'", "exact")]
#[case("age > 30", "conservative")]
#[case("country = 'DE' AND age > 30", "conservative")]
#[case("country = 'DE' OR age > 30", "incomplete")]
fn text_shows_global_confidence(#[case] predicate: &str, #[case] expected: &str) {
    cmd()
        .args([&test_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "confidence:     {expected}"
        )));
}

/// Text output: predicate analysis block lists every bucket, with `-` when empty.
#[test]
fn text_shows_predicate_analysis_block() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE' AND age > 30"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Predicate Analysis:")
                .and(predicate::str::contains("partition-safe: country = 'DE'"))
                .and(predicate::str::contains("stats-safe:     age > 30"))
                .and(predicate::str::contains("unsplittable:   -")),
        );
}

/// Text output: each phase title carries its own [confidence] tag.
#[rstest]
#[case("country = 'DE'", "Phase 1: Partition pruning [exact]")]
#[case(
    "age > 30",
    "Phase 1: Data skipping (min/max statistics) [conservative]"
)]
#[case(
    "country = 'DE' OR age > 30",
    "Phase 1: Data skipping (min/max statistics) [incomplete]"
)]
fn text_phase_title_carries_confidence_tag(
    #[case] predicate: &str,
    #[case] expected_phase_line: &str,
) {
    cmd()
        .args([&test_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(expected_phase_line));
}

/// Text output: a two-phase run tags both phases independently.
#[test]
fn text_two_phases_each_carry_confidence_tag() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE' AND age > 40"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning [exact]").and(
                predicate::str::contains(
                    "Phase 2: Data skipping (min/max statistics) [conservative]",
                ),
            ),
        );
}

/// Text output: unsplittable predicate triggers a Warnings section with the note code.
#[test]
fn text_warnings_section_shows_unsplittable_note() {
    cmd()
        .args([&test_table(), "-w", "country = 'DE' OR age > 30"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Warnings!").and(predicate::str::contains("UNSPLITTABLE_OR")),
        );
}

/// JSON output: top-level analysis.confidence reflects the bucket assignment.
#[rstest]
#[case("country = 'DE'", "exact")]
#[case("age > 30", "conservative")]
#[case("country = 'DE' AND age > 30", "conservative")]
#[case("country = 'DE' OR age > 30", "incomplete")]
fn json_analysis_confidence(#[case] predicate: &str, #[case] expected: &str) {
    let output = cmd()
        .args([&test_table(), "-w", predicate, "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["analysis"]["confidence"], expected);
}

/// JSON output: full analysis block shape on an unsplittable predicate.
#[test]
fn json_analysis_block_has_all_fields() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE' OR age > 30",
            "--format",
            "json",
        ])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert!(json["analysis"]["partition_safe"].is_null());
    assert!(json["analysis"]["stats_safe"].is_null());
    assert_eq!(
        json["analysis"]["unsplittable"],
        "country = 'DE' OR age > 30"
    );
    assert_eq!(json["analysis"]["confidence"], "incomplete");
    assert_eq!(json["analysis"]["notes"][0]["code"], "UNSPLITTABLE_OR");
}

/// JSON output: each phase carries its own confidence string.
#[rstest]
#[case("country = 'DE'", 0, "exact")]
#[case("age > 30", 0, "conservative")]
#[case("country = 'DE' AND age > 40", 0, "exact")]
#[case("country = 'DE' AND age > 40", 1, "conservative")]
fn json_phase_confidence(
    #[case] predicate: &str,
    #[case] phase_idx: usize,
    #[case] expected: &str,
) {
    let output = cmd()
        .args([&test_table(), "-w", predicate, "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["phases"][phase_idx]["confidence"], expected);
}

// ── JSON schema (Step 1.3; schema_version bumps additively) ────────

fn run_json(args: &[&str]) -> serde_json::Value {
    let output = cmd().args(args).output().unwrap();
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn json_carries_schema_and_tool_version() {
    let json = run_json(&[&test_table(), "--format", "json"]);
    assert_eq!(json["schema_version"], "0.2.0");
    assert_eq!(json["tool_version"], env!("CARGO_PKG_VERSION"));
}

#[test]
fn json_carries_elapsed_ms() {
    let json = run_json(&[&test_table(), "--format", "json"]);
    let elapsed = json["elapsed_ms"].as_u64().expect("elapsed_ms must be u64");
    // The full flow runs in well under 1 minute on any fixture.
    assert!(elapsed < 60_000);
}

/// `phases[]` no longer carries per-file detail in the stable schema.
#[test]
fn json_phases_have_no_files_field() {
    let json = run_json(&[&test_table(), "-w", "country = 'DE'", "--format", "json"]);
    let phase = &json["phases"][0];
    assert!(
        phase.get("files").is_none(),
        "per-file detail lives top-level behind --verbose, never per phase"
    );
}

#[test]
fn json_assertions_empty_when_no_flags() {
    let json = run_json(&[&test_table(), "-w", "country = 'DE'", "--format", "json"]);
    let assertions = json["assertions"]
        .as_array()
        .expect("assertions must be an array");
    assert!(assertions.is_empty());
    assert!(json["result"].is_null());
}

#[test]
fn json_assertions_populated_with_min_pruning() {
    let json = run_json(&[
        &test_table(),
        "-w",
        "country = 'DE'",
        "--format",
        "json",
        "--min-pruning",
        "50",
    ]);
    let assertions = json["assertions"].as_array().unwrap();
    assert_eq!(assertions.len(), 1);
    assert_eq!(assertions[0]["name"], "min_pruning");
    assert_eq!(assertions[0]["threshold"], 50.0);
    assert_eq!(assertions[0]["result"], "pass");
    assert_eq!(json["result"], "pass");
}

#[test]
fn json_assertions_populated_with_assert_stats() {
    let json = run_json(&[&test_table(), "--format", "json", "--assert-stats"]);
    let assertions = json["assertions"].as_array().unwrap();
    assert_eq!(assertions.len(), 1);
    assert_eq!(assertions[0]["name"], "stats_complete");
    assert_eq!(assertions[0]["missing_count"], 0);
    assert_eq!(assertions[0]["result"], "pass");
    assert_eq!(json["result"], "pass");
}

#[test]
fn json_result_fail_when_assertion_fails() {
    let output = cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE'",
            "--format",
            "json",
            "--min-pruning",
            "99",
        ])
        .output()
        .unwrap();
    // Process exits 1 on assertion failure, but the JSON is still produced first.
    assert!(!output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["assertions"][0]["result"], "fail");
    assert_eq!(json["result"], "fail");
}

#[test]
fn json_stats_mode_absent_for_empty_table() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let empty = format!("{manifest_dir}/fixtures/test-table-empty");
    let json = run_json(&[&empty, "--format", "json"]);
    assert_eq!(json["stats"]["mode"], "absent");
    assert_eq!(json["stats"]["files_with_stats"], 0);
    assert_eq!(json["stats"]["total_files"], 0);
}

// ── Per-file JSON detail (--verbose) and --limit ────────────────────

#[test]
fn json_without_verbose_has_no_files_array() {
    let json = run_json(&[&test_table(), "-w", "country = 'DE'", "--format", "json"]);
    assert!(json.get("files").is_none());
    assert!(json.get("files_truncated").is_none());
}

#[test]
fn json_verbose_lists_every_file_with_outcome() {
    let json = run_json(&[
        &test_table(),
        "-w",
        "country = 'DE' AND age > 40",
        "--format",
        "json",
        "--verbose",
    ]);
    let files = json["files"].as_array().unwrap();
    assert_eq!(files.len(), 6);
    assert_eq!(json["files_truncated"], false);

    let kept: Vec<&serde_json::Value> = files.iter().filter(|f| f["kept"] == true).collect();
    assert_eq!(kept.len(), 1);
    assert!(kept[0]["pruned_by"].is_null());
    assert_eq!(kept[0]["partition_values"]["country"], "DE");
    assert_eq!(kept[0]["has_stats"], true);

    let partition_dropped = files
        .iter()
        .filter(|f| f["pruned_by"] == "Partition pruning")
        .count();
    let skipping_dropped = files
        .iter()
        .filter(|f| f["pruned_by"] == "Data skipping (min/max statistics)")
        .count();
    assert_eq!(partition_dropped, 4);
    assert_eq!(skipping_dropped, 1);
}

#[test]
fn json_limit_caps_the_files_array() {
    let json = run_json(&[
        &test_table(),
        "-w",
        "country = 'DE'",
        "--format",
        "json",
        "--verbose",
        "--limit",
        "2",
    ]);
    assert_eq!(json["files"].as_array().unwrap().len(), 2);
    assert_eq!(json["files_truncated"], true);
}

#[test]
fn text_limit_truncates_the_listing_with_a_tail_note() {
    cmd()
        .args([
            &test_table(),
            "-w",
            "country = 'DE'",
            "--verbose",
            "--limit",
            "2",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("... and 4 more files"));
}

// ── Negative paths: fail loudly, cleanly, and with empty stdout ─────

#[test]
fn unknown_column_fails_with_a_named_error() {
    cmd()
        .args([&test_table(), "-w", "nosuchcol = 5"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unknown column: nosuchcol"));
}

#[rstest]
#[case("age = 'abc'")] // string literal against an int column
#[case("age = 99999999999999999999999")] // parses only as f64, int column
fn type_mismatched_literal_fails_loudly(#[case] predicate_str: &str) {
    // Loud failure over silent keep-all: a predicate the kernel cannot
    // type-check must not report fake pruning numbers.
    cmd()
        .args([&test_table(), "-w", predicate_str])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn invalid_format_value_is_a_usage_error() {
    cmd()
        .args([&test_table(), "--format", "jsn"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("possible values"));
}

#[test]
fn min_pruning_without_where_is_a_usage_error() {
    // Previously this ran and failed as "total pruning 0.0% is below
    // threshold", hiding the real mistake (no predicate was given).
    cmd()
        .args([&test_table(), "--min-pruning", "50"])
        .assert()
        .code(2)
        .stderr(predicate::str::contains("--where"));
}

#[test]
fn nonexistent_table_path_fails_cleanly() {
    cmd()
        .arg("/nonexistent/delta-table")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid path"));
}

#[test]
fn errors_leave_stdout_empty_in_json_mode() {
    // CI consumers pipe stdout to jq; a failure must never emit a partial
    // document there.
    let output = cmd()
        .args([&test_table(), "-w", "((", "--format", "json"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

// A consumer that stops reading (closed stdout, e.g. `... | head` or a
// crashed jq) must not crash the run: output stops, stderr stays clean,
// and the exit code still reflects the gate verdict.
mod closed_stdout {
    use std::process::{Command as StdCommand, Stdio};

    use crate::common::{LogBuilder, int_range_stats};

    fn big_table() -> crate::common::TempTable {
        // Enough verbose output to overflow any pipe buffer, so writes
        // keep happening after the reader is gone.
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

    fn run_with_closed_stdout(table: &str, extra: &[&str]) -> std::process::Output {
        let mut child = StdCommand::new(assert_cmd::cargo::cargo_bin("delta-explain"))
            .arg(table)
            .args(["-w", "country = 'C3'", "--verbose"])
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        drop(child.stdout.take());
        child.wait_with_output().unwrap()
    }

    #[test]
    fn no_panic_and_exit_zero_without_gates() {
        let table = big_table();
        let out = run_with_closed_stdout(&table.path(), &[]);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stderr.contains("panicked"), "panic on stderr: {stderr}");
        assert_eq!(out.status.code(), Some(0), "stderr: {stderr}");
    }

    #[test]
    fn failing_gate_still_exits_one() {
        let table = big_table();
        let out = run_with_closed_stdout(&table.path(), &["--min-pruning", "99.9"]);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(!stderr.contains("panicked"), "panic on stderr: {stderr}");
        assert_eq!(out.status.code(), Some(1), "stderr: {stderr}");
        assert!(stderr.contains("ASSERTION FAILED"), "stderr: {stderr}");
    }
}
