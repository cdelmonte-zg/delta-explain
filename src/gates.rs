mod context;
mod min_pruning;
mod model;
mod stats_complete;

use crate::analysis::model::AnalysisResult;
use crate::table::TableState;

use model::GateContext;

pub use model::{AssertionResult, GateConfig, GateOutcome, GateStatus};

pub(crate) fn context(table: &TableState, analysis: Option<&AnalysisResult>) -> GateContext {
    context::build(table, analysis)
}

pub(crate) fn evaluate(context: GateContext, config: GateConfig) -> GateOutcome {
    let mut assertions = Vec::new();

    if let Some(threshold) = config.min_pruning {
        assertions.push(min_pruning::evaluate(min_pruning::Input {
            total_files: context.total_files,
            final_files: context.final_files,
            threshold,
        }));
    }

    if config.assert_stats {
        assertions.push(stats_complete::evaluate(stats_complete::Input {
            missing_files: context.missing_stats_files,
        }));
    }

    let overall = overall(&assertions);

    GateOutcome {
        assertions,
        overall,
    }
}

fn overall(assertions: &[AssertionResult]) -> Option<GateStatus> {
    if assertions.is_empty() {
        return None;
    }

    if assertions
        .iter()
        .all(|assertion| assertion.status() == GateStatus::Pass)
    {
        Some(GateStatus::Pass)
    } else {
        Some(GateStatus::Fail)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_requested_gate_has_no_verdict() {
        let outcome = evaluate(
            GateContext {
                total_files: 4,
                final_files: 1,
                missing_stats_files: vec![],
            },
            GateConfig::default(),
        );

        assert!(outcome.assertions.is_empty());
        assert_eq!(outcome.overall, None);
    }

    #[test]
    fn both_gates_can_pass() {
        let outcome = evaluate(
            GateContext {
                total_files: 4,
                final_files: 1,
                missing_stats_files: vec![],
            },
            GateConfig {
                min_pruning: Some(60.0),
                assert_stats: true,
            },
        );

        assert_eq!(outcome.assertions.len(), 2);

        assert_eq!(outcome.overall, Some(GateStatus::Pass));
    }

    #[test]
    fn one_failed_gate_fails_overall() {
        let missing = vec!["c.parquet".to_string()];

        let outcome = evaluate(
            GateContext {
                total_files: 4,
                final_files: 1,
                missing_stats_files: missing,
            },
            GateConfig {
                min_pruning: Some(60.0),
                assert_stats: true,
            },
        );

        assert_eq!(outcome.assertions[0].status(), GateStatus::Pass);

        assert_eq!(outcome.assertions[1].status(), GateStatus::Fail);

        assert_eq!(outcome.overall, Some(GateStatus::Fail));

        assert!(outcome.failed());
    }
}
