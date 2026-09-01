use super::model::{AssertionResult, GateStatus};

pub(super) struct Input {
    pub missing_files: Vec<String>,
}

pub(super) fn evaluate(input: Input) -> AssertionResult {
    let status = if input.missing_files.is_empty() {
        GateStatus::Pass
    } else {
        GateStatus::Fail
    };

    AssertionResult::StatsComplete {
        missing_files: input.missing_files,
        status,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_when_every_file_has_stats() {
        let result = evaluate(Input {
            missing_files: vec![],
        });

        assert_eq!(
            result,
            AssertionResult::StatsComplete {
                missing_files: Vec::new(),
                status: GateStatus::Pass,
            }
        );
    }

    #[test]
    fn fails_and_preserves_missing_paths() {
        let missing = vec!["c.parquet".to_string(), "d.parquet".to_string()];

        let result = evaluate(Input {
            missing_files: missing.clone(),
        });

        assert_eq!(
            result,
            AssertionResult::StatsComplete {
                missing_files: missing,
                status: GateStatus::Fail,
            }
        );
    }
}
