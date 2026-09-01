use super::model::{AssertionResult, GateStatus};

pub(super) struct Input {
    pub total_files: usize,
    pub final_files: usize,
    pub threshold: f64,
}

pub(super) fn evaluate(input: Input) -> AssertionResult {
    let actual = pruning_pct(input.total_files, input.final_files);

    let status = if actual >= input.threshold {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    };

    AssertionResult::MinPruning {
        threshold: input.threshold,
        actual,
        status,
    }
}

fn pruning_pct(total_files: usize, final_files: usize) -> f64 {
    if total_files == 0 {
        return 0.0;
    }

    let dropped = total_files.saturating_sub(final_files);

    dropped as f64 / total_files as f64 * 100.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_when_actual_meets_threshold() {
        let result = evaluate(Input {
            total_files: 4,
            final_files: 1,
            threshold: 60.0,
        });

        assert_eq!(
            result,
            AssertionResult::MinPruning {
                threshold: 60.0,
                actual: 75.0,
                status: GateStatus::Pass,
            }
        );
    }

    #[test]
    fn fails_when_actual_is_below_threshold() {
        let result = evaluate(Input {
            total_files: 4,
            final_files: 1,
            threshold: 90.0,
        });

        assert_eq!(
            result,
            AssertionResult::MinPruning {
                threshold: 90.0,
                actual: 75.0,
                status: GateStatus::Fail,
            }
        );
    }

    #[test]
    fn empty_table_has_zero_pruning() {
        let result = evaluate(Input {
            total_files: 0,
            final_files: 0,
            threshold: 1.0,
        });

        assert_eq!(
            result,
            AssertionResult::MinPruning {
                threshold: 1.0,
                actual: 0.0,
                status: GateStatus::Fail,
            }
        );
    }
}
