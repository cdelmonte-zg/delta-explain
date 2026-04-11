use assert_cmd::Command;
use predicates::prelude::*;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn test_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/test-table")
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
        .stdout(
            predicate::str::contains("[KEPT   ]")
                .and(predicate::str::contains("[DROPPED]")),
        );
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
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--verbose"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("stats(age:")
                .and(predicate::str::contains("..")),
        );
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
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--format", "json"])
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
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--format", "json"])
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
    let statuses: Vec<&str> = files.iter().map(|f| f["status"].as_str().unwrap()).collect();
    assert!(statuses.contains(&"kept"));
    assert!(statuses.contains(&"dropped"));
}

// ── CI assertions ───────────────────────────────────────────────────

#[test]
fn min_pruning_passes_when_above_threshold() {
    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--min-pruning", "50"])
        .assert()
        .success();
}

#[test]
fn min_pruning_fails_when_below_threshold() {
    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--min-pruning", "90"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("ASSERTION FAILED"));
}

#[test]
fn min_pruning_exact_boundary() {
    // 83.33% pruning — threshold 83 should pass, 84 should fail
    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--min-pruning", "83"])
        .assert()
        .success();

    cmd()
        .args([&test_table(), "-w", "age > 40 AND country = 'DE'", "--min-pruning", "84"])
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
            &test_table(), "-w", "age > 40 AND country = 'DE'",
            "--format", "json", "--min-pruning", "50",
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
    cmd()
        .args(["./does-not-exist"])
        .assert()
        .failure();
}
