use super::predicate::Pred;

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
    use crate::v2::analysis::predicate;

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
}
