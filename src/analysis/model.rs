use std::collections::HashSet;

use super::predicate::Pred;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalysisResult {
    pub classification: PredicateClassification,
    pub partition: PartitionAnalysis,
    pub scan: ScanAnalysis,
}

/// Maximum confidence allowed by static predicate classification.
///
/// Runtime evidence may later lower this confidence, for example when
/// statistics required by a stats-safe fragment are unavailable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Confidence {
    Exact,
    Conservative,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAnalysis {
    /// Files surviving all partition pruning strategies.
    ///
    /// `None` means that the predicate contained no partition-only fragment,
    /// so the partition phase was not executed.
    pub survivors: Option<HashSet<String>>,

    /// Files kept conservatively because an exact partition fragment could
    /// not be evaluated against their serialized partition values.
    pub evaluation_gaps: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhaseKind {
    PartitionPruning,
    DataSkipping,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseAnalysis {
    pub kind: PhaseKind,
    pub confidence: Confidence,
    pub input_count: usize,
    pub output_count: usize,
    pub surviving_paths: HashSet<String>,
}

/// Static classification of a normalized predicate.
///
/// The classifier operates on top-level conjuncts and places each one in
/// exactly one bucket.
///
/// No rendering, diagnostics, kernel execution, or runtime evidence belongs
/// in this type.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PredicateClassification {
    /// Fragments referencing partition columns only and directly lowerable
    /// to the kernel predicate language.
    pub partition_safe: Vec<Pred>,

    /// Fragments referencing partition columns only whose semantics are
    /// known but which cannot be lowered by the general kernel predicate
    /// path. These can be evaluated exactly against literal partition
    /// values.
    pub partition_exact: Vec<Pred>,

    /// Fragments referencing non-partition columns only and suitable for
    /// statistics-based pruning.
    pub stats_safe: Vec<Pred>,

    /// Fragments that cannot be cleanly attributed to one pruning axis.
    pub unsplittable: Vec<UnsplittableFragment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanAnalysis {
    /// Files surviving the general kernel pruning phase.
    ///
    /// `None` means that the predicate was completely handled by the
    /// partition phase and no general scan phase was required.
    pub survivors: Option<HashSet<String>>,
}

/// What happens to an unsplittable fragment.
///
/// A mixed partition/data expression may still be understood by the kernel
/// and therefore participate in the final scan, even though its pruning
/// cannot be attributed to a single phase.
///
/// An unsupported expression cannot safely participate in pruning and is
/// therefore stripped from the scan predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsplittableHandling {
    Scanned,
    Stripped,
}

/// A predicate fragment that cannot be attributed exclusively to either
/// partition pruning or statistics pruning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsplittableFragment {
    pub predicate: Pred,
    pub handling: UnsplittableHandling,
}

impl PredicateClassification {
    /// Confidence ceiling implied by predicate structure alone.
    ///
    /// Runtime analysis can later lower this value, but should not raise it.
    pub fn confidence_ceiling(&self) -> Confidence {
        if !self.unsplittable.is_empty() {
            Confidence::Incomplete
        } else if !self.stats_safe.is_empty() {
            Confidence::Conservative
        } else {
            Confidence::Exact
        }
    }

    pub fn partition_exact_predicate(&self) -> Option<Pred> {
        conjunction(&self.partition_exact)
    }

    /// Compose all kernel-lowerable partition fragments into one predicate.
    ///
    /// The classification remains stored as individual fragments; the
    /// conjunction is derived only when an executable partition predicate
    /// is needed.
    pub fn partition_safe_predicate(&self) -> Option<Pred> {
        conjunction(&self.partition_safe)
    }

    /// Whether analysis needs the general kernel pruning phase.
    ///
    /// Pure partition predicates are completely handled by partition pruning.
    /// Stats predicates and unsplittable fragments require a second phase.
    pub fn requires_scan_phase(&self) -> bool {
        !self.stats_safe.is_empty() || !self.unsplittable.is_empty()
    }

    /// Compose the fragments that can participate in the general kernel scan.
    ///
    /// Partition-exact fragments are intentionally excluded because they cannot
    /// be lowered to the kernel predicate language.
    ///
    /// Stripped unsplittable fragments are excluded because their semantics
    /// cannot be represented safely.
    pub fn scan_predicate(&self) -> Option<Pred> {
        let predicates = self
            .partition_safe
            .iter()
            .chain(self.stats_safe.iter())
            .chain(
                self.unsplittable
                    .iter()
                    .filter(|fragment| fragment.handling == UnsplittableHandling::Scanned)
                    .map(|fragment| &fragment.predicate),
            )
            .cloned()
            .collect::<Vec<_>>();

        conjunction(&predicates)
    }

    /// Number of fragments removed from pruning because their semantics
    /// cannot be represented safely.
    pub fn stripped_count(&self) -> usize {
        self.unsplittable
            .iter()
            .filter(|fragment| fragment.handling == UnsplittableHandling::Stripped)
            .count()
    }
}

fn conjunction(predicates: &[Pred]) -> Option<Pred> {
    match predicates {
        [] => None,
        [single] => Some(single.clone()),
        many => Some(Pred::And(many.to_vec())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::predicate;

    #[test]
    fn single_partition_fragment_is_returned_directly() {
        let pred = predicate::parse("country = 'DE'").unwrap();

        let classification = PredicateClassification {
            partition_safe: vec![pred],
            ..Default::default()
        };

        assert_eq!(
            classification
                .partition_safe_predicate()
                .unwrap()
                .to_string(),
            "country = 'DE'"
        );
    }

    #[test]
    fn multiple_partition_fragments_are_conjoined() {
        let first = predicate::parse("country >= 'DE'").unwrap();
        let second = predicate::parse("country < 'DF'").unwrap();

        let classification = PredicateClassification {
            partition_safe: vec![first, second],
            ..Default::default()
        };

        assert_eq!(
            classification
                .partition_safe_predicate()
                .unwrap()
                .to_string(),
            "country >= 'DE' AND country < 'DF'"
        );
    }

    #[test]
    fn scan_predicate_contains_kernel_lowerable_fragments() {
        let partition = predicate::parse("country = 'DE'").unwrap();

        let stats = predicate::parse("age > 20").unwrap();

        let scanned = predicate::parse("country = 'DE' OR age > 30").unwrap();

        let stripped = predicate::parse("UPPER(name) = 'X'").unwrap();

        let classification = PredicateClassification {
            partition_safe: vec![partition],
            stats_safe: vec![stats],
            unsplittable: vec![
                UnsplittableFragment {
                    predicate: scanned,
                    handling: UnsplittableHandling::Scanned,
                },
                UnsplittableFragment {
                    predicate: stripped,
                    handling: UnsplittableHandling::Stripped,
                },
            ],
            ..Default::default()
        };

        assert_eq!(
            classification.scan_predicate().unwrap().to_string(),
            "country = 'DE' AND age > 20 AND (country = 'DE' OR age > 30)"
        );
    }

    #[test]
    fn pure_partition_predicate_does_not_require_scan_phase() {
        let classification = PredicateClassification {
            partition_safe: vec![predicate::parse("country = 'DE'").unwrap()],
            ..Default::default()
        };

        assert!(!classification.requires_scan_phase());
    }
}
