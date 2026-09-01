#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateStatus {
    Pass,
    Fail,
}

impl GateStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            GateStatus::Pass => "pass",
            GateStatus::Fail => "fail",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AssertionResult {
    MinPruning {
        threshold: f64,
        actual: f64,
        status: GateStatus,
    },

    StatsComplete {
        missing_files: Vec<String>,
        status: GateStatus,
    },
}

impl AssertionResult {
    pub fn name(&self) -> &'static str {
        match self {
            AssertionResult::MinPruning { .. } => "min_pruning",

            AssertionResult::StatsComplete { .. } => "stats_complete",
        }
    }

    pub fn status(&self) -> GateStatus {
        match self {
            AssertionResult::MinPruning { status, .. }
            | AssertionResult::StatsComplete { status, .. } => *status,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GateConfig {
    pub min_pruning: Option<f64>,
    pub assert_stats: bool,
}

/// Stable facts made available to the gate facade.
///
/// Individual gates do not receive this whole context. The facade extracts
/// only the fields required by each gate-specific evaluator.
#[derive(Debug, Clone)]
pub struct GateContext {
    pub(crate) total_files: usize,
    pub(crate) final_files: usize,
    pub(crate) missing_stats_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GateOutcome {
    pub assertions: Vec<AssertionResult>,
    pub overall: Option<GateStatus>,
}

impl GateOutcome {
    pub fn failed(&self) -> bool {
        self.overall == Some(GateStatus::Fail)
    }
}
