//! Turn survivor sets into attributed pruning phases.
//!
//! Pure: the comparative scans happen in [`crate::scan`]; this module encodes
//! the attribution arithmetic on their results. Phase 1 is the drop from the
//! baseline to the partition-only scan (partition pruning), Phase 2 the drop
//! from there to the full-predicate scan (data skipping). It is a
//! decomposition, not an execution order.

use std::collections::HashSet;

use crate::predicate_analyzer::{Confidence, PredicateAnalysis};
use crate::report::PhaseResult;

/// Build the attributed phases from the survivor sets of the comparative
/// scans. Callers pass `None` for a scan they did not run: the partition-only
/// scan exists only when the analysis produced a `partition_safe` fragment,
/// the full scan only when a `stats_safe` or `unsplittable` fragment exists.
pub fn build_phases(
    analysis: &PredicateAnalysis,
    total_files: usize,
    partition_survivors: Option<HashSet<String>>,
    full_survivors: Option<HashSet<String>>,
) -> Vec<PhaseResult> {
    let mut phases = Vec::new();
    let mut prev_count = total_files;

    // Phase 1 covers both partition routes: the kernel scan on the
    // partition-safe fragment and the literal evaluation of the
    // partition-exact one; the caller intersects their survivor sets.
    let partition_display = [&analysis.partition_safe, &analysis.partition_exact]
        .iter()
        .filter_map(|f| f.as_ref())
        .cloned()
        .collect::<Vec<_>>()
        .join(" AND ");

    if let (false, Some(survivors)) = (partition_display.is_empty(), partition_survivors) {
        let count = survivors.len();
        phases.push(PhaseResult {
            confidence: Confidence::Exact,
            name: "Partition pruning".into(),
            predicate_display: partition_display,
            input_count: prev_count,
            output_count: count,
            surviving_paths: survivors,
        });
        prev_count = count;
    }

    if let Some(survivors) = full_survivors {
        let display = [&analysis.stats_safe, &analysis.unsplittable]
            .iter()
            .filter_map(|f| f.as_ref())
            .cloned()
            .collect::<Vec<_>>()
            .join(" AND ");

        phases.push(PhaseResult {
            name: "Data skipping (min/max statistics)".into(),
            // The kernel reasons over the whole predicate; when part of it is
            // unsplittable the delta from the partition-only scan is no longer
            // a clean attribution to data skipping alone.
            confidence: if analysis.unsplittable.is_some() {
                Confidence::Incomplete
            } else {
                Confidence::Conservative
            },
            predicate_display: display,
            input_count: prev_count,
            output_count: survivors.len(),
            surviving_paths: survivors,
        });
    }

    phases
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::predicate_analyzer::analyze;

    fn paths(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn mixed_predicate_builds_two_chained_phases() {
        let analysis = analyze("country = 'DE' AND age > 40", &["country".into()]).unwrap();

        let phases = build_phases(&analysis, 6, Some(paths(&["a", "b"])), Some(paths(&["a"])));

        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "Partition pruning");
        assert_eq!(phases[0].confidence, Confidence::Exact);
        assert_eq!((phases[0].input_count, phases[0].output_count), (6, 2));
        // Phase 2 input chains from phase 1 output
        assert_eq!((phases[1].input_count, phases[1].output_count), (2, 1));
        assert_eq!(phases[1].confidence, Confidence::Conservative);
    }

    #[test]
    fn partition_only_predicate_builds_single_exact_phase() {
        let analysis = analyze("country = 'DE'", &["country".into()]).unwrap();

        let phases = build_phases(&analysis, 6, Some(paths(&["a", "b"])), None);

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].confidence, Confidence::Exact);
        assert_eq!((phases[0].input_count, phases[0].output_count), (6, 2));
    }

    #[test]
    fn stats_only_predicate_builds_single_phase_from_baseline() {
        let analysis = analyze("age > 40", &["country".into()]).unwrap();

        let phases = build_phases(&analysis, 6, None, Some(paths(&["a", "b", "c", "d"])));

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].name, "Data skipping (min/max statistics)");
        assert_eq!((phases[0].input_count, phases[0].output_count), (6, 4));
        assert_eq!(phases[0].confidence, Confidence::Conservative);
    }

    #[test]
    fn unsplittable_fragment_marks_phase_incomplete() {
        let analysis = analyze("country = 'DE' OR age > 40", &["country".into()]).unwrap();
        assert!(analysis.unsplittable.is_some());

        let phases = build_phases(&analysis, 6, None, Some(paths(&["a"])));

        assert_eq!(phases.len(), 1);
        assert_eq!(phases[0].confidence, Confidence::Incomplete);
    }
}
