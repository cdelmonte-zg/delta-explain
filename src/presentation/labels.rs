use crate::analysis::model::{Confidence, PhaseKind};

pub(super) fn phase_name(kind: PhaseKind) -> &'static str {
    match kind {
        PhaseKind::PartitionPruning => "Partition pruning",

        PhaseKind::DataSkipping => "Data skipping (min/max statistics)",
    }
}

pub(super) fn confidence_label(confidence: Confidence) -> &'static str {
    match confidence {
        Confidence::Exact => "exact",

        Confidence::Conservative => "conservative",

        Confidence::Incomplete => "incomplete",
    }
}
