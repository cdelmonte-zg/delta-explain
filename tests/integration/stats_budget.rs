//! Integration tests for the `test-table-stats-budget` fixture.
//!
//! Delta indexes only the first `delta.dataSkippingNumIndexedCols` *leaf*
//! columns (default 32); each nested struct leaf counts as one. The fixture
//! sets the property to 4 and uses a 5-leaf struct `s` (a..e) followed by a
//! root column `tail`. So `s.a..s.d` carry stats, while `s.e` and `tail` do
//! not; a predicate on those cannot be skipped even though the values would
//! allow it. This is the practical ceiling of nested data skipping: a deep or
//! wide struct can starve later columns of statistics.

use crate::common::{cmd, fixture};
use predicates::prelude::*;
use rstest::rstest;

fn budget_table() -> String {
    fixture("test-table-stats-budget")
}

#[test]
fn fixture_reports_two_files() {
    cmd()
        .arg(budget_table())
        .assert()
        .success()
        .stdout(predicate::str::contains("Files in snapshot: 2"));
}

/// A leaf within the budget (s.a) carries stats, so the file whose range
/// excludes the predicate is skipped. A leaf past the budget (s.e) and the
/// trailing root column (tail) have no stats, so no file can be ruled out.
#[rstest]
#[case("s.a > 500", 1)] // in-budget leaf -> skips one file
#[case("s.e > 500", 2)] // out-of-budget leaf -> no skipping
#[case("tail > 500", 2)] // out-of-budget root column -> no skipping
fn budget_limits_skipping(#[case] predicate: &str, #[case] remaining: usize) {
    cmd()
        .args([&budget_table(), "-w", predicate])
        .assert()
        .success()
        .stdout(predicate::str::contains(format!(
            "files remaining: {remaining}"
        )));
}

/// The struct leaves that fit the budget are reported under dotted keys; the
/// ones past it (s.e, tail) simply never appear in the stats line.
#[test]
fn verbose_shows_only_in_budget_leaves() {
    cmd()
        .args([&budget_table(), "-w", "s.a > 500", "--verbose"])
        .assert()
        .success()
        .stdout(predicate::str::contains("s.a:"))
        .stdout(predicate::str::contains("s.d:"))
        .stdout(predicate::str::contains("s.e:").not())
        .stdout(predicate::str::contains("tail:").not());
}
