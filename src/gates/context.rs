use crate::analysis::model::AnalysisResult;
use crate::table::TableState;

use super::model::GateContext;

pub(super) fn build(table: &TableState, analysis: Option<&AnalysisResult>) -> GateContext {
    let total_files = table.metadata.baseline.files.len();

    let final_files = analysis
        .and_then(|result| {
            result
                .scan
                .survivors
                .as_ref()
                .or(result.partition.survivors.as_ref())
        })
        .map(|survivors| survivors.len())
        .unwrap_or(total_files);

    let missing_stats_files = table
        .metadata
        .baseline
        .files
        .iter()
        .filter(|file| !table.metadata.baseline.stats.contains_key(&file.path))
        .map(|file| file.path.clone())
        .collect();

    GateContext {
        total_files,
        final_files,
        missing_stats_files,
    }
}
