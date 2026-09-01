use serde_json::{Value, json};

use crate::v2::error::Result;
use crate::v2::presentation::{AssertionView, DiagnosticScope, ExplanationView, Presentation};

pub const SCHEMA_VERSION: &str = "0.4.0";

pub(super) fn render(presentation: &Presentation) -> Result<String> {
    let analysis_notes = presentation
        .warnings
        .iter()
        .filter(|warning| warning.scope == DiagnosticScope::Analysis)
        .map(|warning| {
            json!({
                "code":
                    warning.code,

                "message":
                    warning.message,
            })
        })
        .collect::<Vec<_>>();

    let table_notes = presentation
        .warnings
        .iter()
        .filter(|warning| warning.scope == DiagnosticScope::Table)
        .map(|warning| {
            json!({
                "code":
                    warning.code,

                "message":
                    warning.message,
            })
        })
        .collect::<Vec<_>>();

    let analysis = presentation.analysis.as_ref().map(|analysis| {
        json!({
            "partition_safe":
                analysis
                    .partition_safe,

            "partition_exact":
                analysis
                    .partition_exact,

            "stats_safe":
                analysis
                    .stats_safe,

            "unsplittable":
                analysis
                    .unsplittable,

            "confidence":
                analysis
                    .confidence,

            "notes":
                analysis_notes,
        })
    });

    let phases = presentation
        .phases
        .iter()
        .map(|phase| {
            json!({
                "name":
                    phase.name,

                "confidence":
                    phase.confidence,

                "predicate":
                    phase.predicate,

                "input_files":
                    phase.input_files,

                "output_files":
                    phase.output_files,

                "pruned_files":
                    phase.pruned_files,

                "pruning_pct":
                    phase.pruning_pct,
            })
        })
        .collect::<Vec<_>>();

    let assertions = presentation
        .assertions
        .iter()
        .map(assertion)
        .collect::<Vec<_>>();

    let features = &presentation.table.features;

    let stats = &presentation.table.stats;

    let mut output = json!({
        "schema_version":
            SCHEMA_VERSION,

        "tool_version":
            env!(
                "CARGO_PKG_VERSION"
            ),

        "elapsed_ms":
            presentation
                .elapsed_ms,

        "table":
            presentation
                .table
                .path,

        "version":
            presentation
                .table
                .version,

        "predicate":
            presentation
                .predicate,

        "total_files":
            presentation
                .table
                .total_files,

        "final_files":
            presentation
                .final_files,

        "total_pruning_pct":
            presentation
                .total_pruning_pct,

        "analysis":
            analysis,

        "table_features": {
            "deletion_vectors": {
                "enabled":
                    features
                        .deletion_vectors_enabled,

                "files_with_deletion_vectors":
                    features
                        .files_with_deletion_vectors,
            },

            "column_mapping_mode":
                features
                    .column_mapping_mode,

            "clustering_columns":
                features
                    .clustering_columns,

            "in_commit_timestamps":
                features
                    .in_commit_timestamps,

            "unrecognized_writer_features":
                features
                    .unrecognized_writer_features,

            "notes":
                table_notes,
        },

        "stats": {
            "mode":
                stats.mode,

            "files_with_stats":
                stats
                    .files_with_stats,

            "total_files":
                stats
                    .total_files,

            "pct":
                stats.pct,
        },

        "phases":
            phases,

        "assertions":
            assertions,

        "result":
            presentation
                .result
                .map(|status| {
                    status.as_str()
                }),
    });

    if let Some(files) = &presentation.files {
        let cap = files.limit.unwrap_or(usize::MAX);

        let rendered_files = files
            .entries
            .iter()
            .take(cap)
            .map(|file| {
                json!({
                    "path":
                        file.path,

                    "size_bytes":
                        file.size_bytes,

                    "partition_values":
                        file.partition_values,

                    "num_records":
                        file.num_records,

                    "has_stats":
                        file.has_stats,

                    "kept":
                        file.kept,

                    "pruned_by":
                        file.pruned_by,
                })
            })
            .collect::<Vec<_>>();

        output["files_truncated"] = json!(files.entries.len() > rendered_files.len());

        output["files"] = json!(rendered_files);
    }

    if let Some(explanations) = &presentation.explanations {
        output["explain"] = json!(explanations.iter().map(explanation).collect::<Vec<_>>());
    }

    Ok(serde_json::to_string_pretty(&output)?)
}

fn assertion(assertion: &AssertionView) -> Value {
    match assertion {
        AssertionView::MinPruning {
            threshold,
            actual,
            status,
        } => {
            json!({
                "name":
                    "min_pruning",

                "threshold":
                    threshold,

                "actual":
                    actual,

                "result":
                    status.as_str(),
            })
        }

        AssertionView::StatsComplete {
            missing_files,
            status,
        } => {
            json!({
                "name":
                    "stats_complete",

                "missing_count":
                    missing_files.len(),

                "result":
                    status.as_str(),
            })
        }
    }
}

fn explanation(explanation: &ExplanationView) -> Value {
    json!({
        "code":
            explanation.code,

        "severity":
            explanation.severity,

        "message":
            explanation.message,

        "suggestion":
            explanation.suggestion,
    })
}
