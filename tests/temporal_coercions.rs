//! Integration tests for temporal and narrow-type literal coercion against
//! the `test-table-temporal` fixture: partitioned by a DATE column, with
//! per-file ranges on TIMESTAMP, DECIMAL(9,2), INT16, and INT8 columns.
//!
//! Without coercion every one of these predicates is a type mismatch the
//! kernel resolves conservatively: zero files pruned. The layout: six files
//! across three date partitions (2026-07-01/02/03), each day a morning file
//! (01:00-03:00) and an evening file (20:00-22:00).

use assert_cmd::Command;
use predicates::prelude::*;
use rstest::rstest;

fn cmd() -> Command {
    Command::cargo_bin("delta-explain").unwrap()
}

fn temporal_table() -> String {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    format!("{manifest_dir}/fixtures/test-table-temporal")
}

// ── DATE: partition pruning ─────────────────────────────────────────

#[rstest]
#[case("event_date = '2026-07-02'")]
#[case("DATE '2026-07-02' = event_date")]
#[case("event_date = DATE '2026-07-02'")]
fn date_literal_prunes_partitions(#[case] predicate: &str) {
    cmd()
        .args([&temporal_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning [exact]").and(
                predicate::str::contains("files remaining: 2  (-4, 67% pruned)"),
            ),
        );
}

#[test]
fn date_range_prunes_partitions() {
    cmd()
        .args([&temporal_table(), "-w", "event_date > '2026-07-02'"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 2  (-4, 67% pruned)",
        ));
}

// ── TIMESTAMP: data skipping ────────────────────────────────────────

#[rstest]
#[case("ts > '2026-07-02 12:00:00'")]
#[case("ts > TIMESTAMP '2026-07-02T12:00:00'")]
#[case("ts > '2026-07-02T12:00:00+00:00'")]
fn timestamp_literal_skips_files(#[case] predicate: &str) {
    // Survivors: day-2 evening, both day-3 files -> 3 of 6.
    cmd()
        .args([&temporal_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 3  (-3, 50% pruned)",
        ));
}

// ── TIMESTAMP_NTZ: wall-clock semantics ─────────────────────────────

#[test]
fn ntz_naive_literal_skips_files() {
    // ts_ntz mirrors ts as wall-clock values, so the same bound survives
    // the same three files.
    cmd()
        .args([&temporal_table(), "-w", "ts_ntz > '2026-07-02 12:00:00'"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 3  (-3, 50% pruned)",
        ));
}

#[test]
fn ntz_offset_literal_is_rejected() {
    cmd()
        .args([
            &temporal_table(),
            "-w",
            "ts_ntz > '2026-07-02T12:00:00+01:00'",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("timezone-naive"));
}

// ── DECIMAL ─────────────────────────────────────────────────────────

#[test]
fn decimal_literal_skips_files() {
    // amount ranges top out at 30/60/90 for the first three files.
    cmd()
        .args([&temporal_table(), "-w", "amount > 100.50"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 3  (-3, 50% pruned)",
        ));
}

#[test]
fn negative_decimal_literal_is_supported() {
    // All amounts are positive, so nothing prunes; the point is that the
    // unary minus coerces instead of erroring.
    cmd()
        .args([&temporal_table(), "-w", "amount > -100.50"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 6  (-0, 0% pruned)",
        ));
}

#[test]
fn decimal_literal_beyond_column_scale_fails_cleanly() {
    cmd()
        .args([&temporal_table(), "-w", "amount > 100.505"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("fractional digits"));
}

// ── Narrow integers ─────────────────────────────────────────────────

#[test]
fn short_literal_skips_files_including_negatives() {
    // small ranges: 10..30, 40..60, 70..90, 100..120, -30..-10, -60..-40
    cmd()
        .args([&temporal_table(), "-w", "small < -15"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 2  (-4, 67% pruned)",
        ));
}

#[test]
fn byte_literal_skips_files() {
    // tiny ranges: 1..3, 4..6, 7..9, 10..12, 13..15, 16..18
    cmd()
        .args([&temporal_table(), "-w", "tiny > 9"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "files remaining: 3  (-3, 50% pruned)",
        ));
}

// ── Mixed: two-phase attribution on a date-partitioned table ────────

#[test]
fn date_partition_plus_timestamp_skipping_chains_two_phases() {
    cmd()
        .args([
            &temporal_table(),
            "-w",
            "event_date = '2026-07-02' AND ts > '2026-07-02 12:00:00'",
        ])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Phase 1: Partition pruning [exact]")
                .and(predicate::str::contains(
                    "files remaining: 2  (-4, 67% pruned)",
                ))
                .and(predicate::str::contains(
                    "Phase 2: Data skipping (min/max statistics)",
                ))
                .and(predicate::str::contains(
                    "files remaining: 1  (-1, 50% pruned)",
                ))
                .and(predicate::str::contains("83% pruned")),
        );
}

// ── Errors stay clean ───────────────────────────────────────────────

#[test]
fn invalid_date_literal_fails_cleanly() {
    cmd()
        .args([&temporal_table(), "-w", "event_date = 'not-a-date'"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("Invalid date"));
}

// ── NULL literal against a DATE column (typed null) ─────────────────

#[rstest]
#[case("event_date IS DISTINCT FROM NULL", 6)] // = IS NOT NULL: every file dated
#[case("event_date IS NOT DISTINCT FROM NULL", 0)] // = IS NULL: prunes everything
fn distinct_from_null_on_date_partition(#[case] predicate: &str, #[case] remaining: u32) {
    cmd()
        .args([&temporal_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "files remaining: {remaining}"
        )));
}
