use delta_kernel::schema::Schema;

use crate::analysis;
use crate::analysis::model::{
    AnalysisResult, ColumnStatsCoverage, Confidence, PhaseAnalysis, PredicateClassification,
};
use crate::diagnostics::{self, ExplainContext, Explanation, Warning, WarningContext};
use crate::metadata::scan::BaselineScan;
use crate::table::TableState;

#[derive(Debug, Clone)]
pub struct Report {
    pub table: TableReport,
    pub predicate: Option<PredicateReport>,

    /// Limitations or anomalies of the analysis itself.
    /// These are rendered unconditionally.
    pub warnings: Vec<Warning>,

    /// Interpretation of pruning effectiveness.
    /// Rendering decides whether to expose these,
    /// e.g. only under `--explain-why`.
    pub explanations: Vec<Explanation>,
}

#[derive(Debug, Clone)]
pub struct TableReport {
    pub path: String,
    pub version: u64,
    pub total_files: usize,
    pub files_with_stats: usize,
    pub partition_columns: Vec<String>,
    pub features: TableFeatureReport,
}

#[derive(Debug, Clone)]
pub struct TableFeatureReport {
    pub deletion_vectors_enabled: bool,
    pub files_with_deletion_vectors: usize,
    pub column_mapping_mode: Option<String>,
    pub clustering_columns: Option<Vec<String>>,
    pub in_commit_timestamps: bool,
    pub unrecognized_writer_features: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PredicateReport {
    pub input: String,
    pub confidence: Confidence,
    pub classification: PredicateClassification,
    pub phases: Vec<PhaseAnalysis>,
    pub partition_evaluation_gaps: usize,
    pub stats_coverage: Option<Vec<ColumnStatsCoverage>>,
}

pub fn build(
    table_path: &str,
    predicate: Option<&str>,
    table: &TableState,
    result: Option<&AnalysisResult>,
) -> Report {
    let features = &table.metadata.features;
    let schema = table.snapshot.schema();

    let table_report = TableReport {
        path: table_path.to_string(),
        version: table.snapshot.version(),
        total_files: table.metadata.baseline.files.len(),
        files_with_stats: table.metadata.baseline.stats.len(),
        partition_columns: table.metadata.partition_columns.clone(),
        features: TableFeatureReport {
            deletion_vectors_enabled: features.deletion_vectors_enabled,
            files_with_deletion_vectors: features.files_with_deletion_vectors,
            column_mapping_mode: features.column_mapping_mode.clone(),
            clustering_columns: features.clustering_columns.clone(),
            in_commit_timestamps: features.in_commit_timestamps,
            unrecognized_writer_features: features.unrecognized_writer_features.clone(),
        },
    };

    let predicate_report = match (predicate, result) {
        (Some(input), Some(result)) => Some(build_predicate_report(
            input,
            result,
            table_report.total_files,
            &table.metadata.baseline,
            schema.as_ref(),
        )),

        _ => None,
    };

    let warnings = diagnostics::warnings::derive(WarningContext {
        analysis: result,
        features: &table.metadata.features,
        total_files: table_report.total_files,
    });

    let explanations = match (result, predicate_report.as_ref()) {
        (Some(result), Some(predicate_report)) => diagnostics::explain::derive(ExplainContext {
            classification: &result.classification,
            phases: &predicate_report.phases,
            partition_columns: &table_report.partition_columns,
            total_files: table_report.total_files,
            files_with_stats: table_report.files_with_stats,
        }),

        _ => Vec::new(),
    };

    Report {
        table: table_report,
        predicate: predicate_report,
        warnings,
        explanations,
    }
}

fn build_predicate_report(
    input: &str,
    result: &AnalysisResult,
    total_files: usize,
    baseline: &BaselineScan,
    schema: &Schema,
) -> PredicateReport {
    PredicateReport {
        input: input.to_string(),
        confidence: analysis::confidence(result),
        classification: result.classification.clone(),
        phases: analysis::phases(result, total_files),
        partition_evaluation_gaps: result.partition.evaluation_gaps,
        stats_coverage: analysis::stats_coverage::compute(
            &result.classification,
            &result.partition,
            baseline,
            schema,
        ),
    }
}
