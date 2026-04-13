use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn test_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/test-table")
}

fn test_table_flat() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/test-table-flat")
}

// ── Basic snapshot ──────────────────────────────────────────────────

#[test]
fn no_predicate_shows_file_count() {
    cmd()
        .arg(&test_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 6"));
}

#[test]
fn no_predicate_shows_version() {
    cmd()
        .arg(&test_table())
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
fn json_contains_stats_coverage() {
    let output = cmd()
        .args([&test_table(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(json["stats_coverage"]["files_with_stats"], 6);
    assert_eq!(json["stats_coverage"]["total_files"], 6);
}

#[test]
fn json_per_file_has_status() {
    let output = cmd()
        .args([&test_table(), "-w", "country = 'DE'", "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let files = json["phases"][0]["files"].as_array().unwrap();
    let statuses: Vec<&str> = files
        .iter()
        .map(|f| f["status"].as_str().unwrap())
        .collect();
    assert!(statuses.contains(&"kept"));
    assert!(statuses.contains(&"dropped"));
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
fn unsupported_function_call() {
    cmd()
        .args([&test_table(), "-w", "UPPER(country) = 'DE'"])
        .assert()
        .failure();
}

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

#[test]
fn subquery_rejected() {
    cmd()
        .args([&test_table(), "-w", "age IN (SELECT 1)"])
        .assert()
        .failure();
}

#[test]
fn like_rejected() {
    cmd()
        .args([&test_table(), "-w", "name LIKE '%Hans%'"])
        .assert()
        .failure();
}

// ── Flat table (no partitions) ─────────────────────────────────────
// These tests use a table with no partition columns and mixed country
// values per file, demonstrating how pruning degrades without
// proper partitioning.

#[test]
fn flat_table_snapshot() {
    cmd()
        .arg(&test_table_flat())
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
            predicate::str::contains("Partition pruning").not()
                .and(predicate::str::contains("Data skipping (min/max statistics)")),
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
    assert!(part_pct > flat_pct, "partitioned ({part_pct}%) should prune more than flat ({flat_pct}%)");
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
                .and(predicate::str::contains("[DROPPED] part-00002.snappy.parquet"))
                .and(predicate::str::contains("[KEPT   ] part-00003.snappy.parquet"))
                .and(predicate::str::contains("[KEPT   ] part-00004.snappy.parquet"))
                .and(predicate::str::contains("[KEPT   ] part-00005.snappy.parquet"))
                .and(predicate::str::contains("[KEPT   ] part-00006.snappy.parquet")),
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
fn flat_json_stats_coverage() {
    let output = cmd()
        .args([&test_table_flat(), "--format", "json"])
        .output()
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();

    assert_eq!(json["stats_coverage"]["files_with_stats"], 6);
    assert_eq!(json["stats_coverage"]["total_files"], 6);
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
