//! Exotic log shapes (VISION v0.5): the kernel handles them, this matrix
//! proves delta-explain does too. Log compaction is synthesized here;
//! multi-part and V2/UUID checkpoints are covered by real fixtures.

use predicates::prelude::*;

use crate::common::{LogBuilder, cmd, int_range_stats};

fn three_commit_builder() -> LogBuilder {
    // 3 commits x 2 files, age ranges disjoint per commit so pruning
    // outcomes are predictable.
    let mut b = LogBuilder::new().column("age", "integer");
    for c in 0..3u32 {
        for f in 0..2u32 {
            let lo = i64::from(c * 20);
            b = b.add_file(
                &format!("part-{c}-{f}.parquet"),
                &[],
                Some(int_range_stats("age", lo, lo + 9, 100)),
            );
        }
        if c < 2 {
            b = b.commit();
        }
    }
    b
}

#[test]
fn multi_commit_log_reads_all_versions() {
    let table = three_commit_builder().build();
    cmd().arg(table.path()).assert().success().stdout(
        predicate::str::contains("Version:     2")
            .and(predicate::str::contains("Files in snapshot: 6")),
    );
}

#[test]
fn compacted_range_alongside_originals_does_not_double_count() {
    // 0..=1 compacted, original commits still present: the reader must
    // prefer one source, never sum both.
    let table = three_commit_builder().compaction(0, 1).build();
    cmd().arg(table.path()).assert().success().stdout(
        predicate::str::contains("Version:     2")
            .and(predicate::str::contains("Files in snapshot: 6")),
    );
}

#[test]
fn pruning_is_identical_with_and_without_compaction() {
    let plain = three_commit_builder().build();
    let compacted = three_commit_builder().compaction(0, 2).build();
    for table in [&plain, &compacted] {
        // age > 45 overlaps only commit 2's ranges (40..49 on both of its
        // files) -> exactly 2 files kept.
        cmd()
            .args([&table.path(), "-w", "age > 45"])
            .assert()
            .success()
            .stdout(predicate::str::contains("files remaining: 2"));
    }
}

#[test]
fn compaction_survives_time_travel_before_its_range_end() {
    let table = three_commit_builder().compaction(0, 2).build();
    cmd()
        .args([&table.path(), "--at-version", "1"])
        .assert()
        .success()
        .stdout(
            predicate::str::contains("Version:     1")
                .and(predicate::str::contains("Files in snapshot: 4")),
        );
}

// ── Real checkpoint shapes (fixtures from create_exotic_checkpoints.py) ──

#[test]
fn multipart_checkpoint_reads_and_prunes() {
    // Classic checkpoint split into two parts
    // (v2.checkpoint.0000000001.0000000002.parquet + part 2 of 2).
    let table = crate::common::fixture("test-table-checkpoint-multipart");
    cmd().arg(&table).assert().success().stdout(
        predicate::str::contains("Version:     2")
            .and(predicate::str::contains("Files in snapshot: 3")),
    );
    cmd()
        .args([&table, "-w", "age > 45"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 1"));
}

#[test]
fn v2_uuid_checkpoint_with_sidecars_reads_and_prunes() {
    // V2 checkpoint: UUID-named JSON manifest plus parquet sidecars in
    // _delta_log/_sidecars.
    let table = crate::common::fixture("test-table-checkpoint-v2");
    cmd().arg(&table).assert().success().stdout(
        predicate::str::contains("Version:     4")
            .and(predicate::str::contains("Files in snapshot: 4")),
    );
    cmd()
        .args([&table, "-w", "age > 45"])
        .assert()
        .success()
        .stdout(predicate::str::contains("files remaining: 2"));
}

#[test]
fn time_travel_across_the_multipart_checkpoint() {
    // Version 1 predates the checkpoint at version 2: the kernel must
    // replay JSON commits only.
    cmd()
        .args([
            &crate::common::fixture("test-table-checkpoint-multipart"),
            "--at-version",
            "1",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 2"));
}

#[test]
fn assert_stats_passes_on_both_checkpoint_shapes() {
    for fixture_name in [
        "test-table-checkpoint-multipart",
        "test-table-checkpoint-v2",
    ] {
        cmd()
            .args([&crate::common::fixture(fixture_name), "--assert-stats"])
            .assert()
            .success();
    }
}
