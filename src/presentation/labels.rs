use crate::analysis::model::{Confidence, PhaseKind, StatsRequirement};

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

pub(super) fn stats_requirement_label(requirement: StatsRequirement) -> &'static str {
    match requirement {
        StatsRequirement::MinMax => "min_max",
        StatsRequirement::NullCount => "null_count",
        StatsRequirement::NullCountAndNumRecords => "null_count_and_num_records",
    }
}
