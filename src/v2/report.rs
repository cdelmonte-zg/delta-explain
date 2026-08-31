use crate::v2::analysis;
use crate::v2::analysis::model::{
    AnalysisResult, Confidence, PhaseAnalysis, PredicateClassification,
};
use crate::v2::diagnostics::{self, Diagnostic};
use crate::v2::table::TableState;

#[derive(Debug, Clone)]
pub struct Report {
    pub table: TableReport,
    pub predicate: Option<PredicateReport>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
pub struct TableReport {
    pub path: String,
    pub version: u64,
    pub total_files: usize,
    pub files_with_stats: usize,
    pub partition_columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PredicateReport {
    pub input: String,
    pub confidence: Confidence,
    pub classification: PredicateClassification,
    pub phases: Vec<PhaseAnalysis>,
    pub partition_evaluation_gaps: usize,
}

pub fn build(
    table_path: &str,
    predicate: Option<&str>,
    table: &TableState,
    result: Option<&AnalysisResult>,
) -> Report {
    let table_report = TableReport {
        path: table_path.to_string(),
        version: table.snapshot.version(),
        total_files: table.metadata.baseline.files.len(),
        files_with_stats: table.metadata.baseline.stats.len(),
        partition_columns: table.metadata.partition_columns.clone(),
    };

    let predicate_report = match (predicate, result) {
        (Some(input), Some(result)) => Some(build_predicate_report(
            input,
            result,
            table_report.total_files,
        )),

        _ => None,
    };

    let diagnostics = result.map(diagnostics::derive).unwrap_or_default();

    Report {
        table: table_report,
        predicate: predicate_report,
        diagnostics,
    }
}

fn build_predicate_report(
    input: &str,
    result: &AnalysisResult,
    total_files: usize,
) -> PredicateReport {
    PredicateReport {
        input: input.to_string(),

        confidence: analysis::confidence(result),

        classification: result.classification.clone(),

        phases: analysis::phases(result, total_files),

        partition_evaluation_gaps: result.partition.evaluation_gaps,
    }
}
