use crate::metadata::scan::BaselineScan;
use crate::report::Report;

use super::labels::phase_name;
use super::model::{ColumnStatsView, FilePhaseState, FileView, FilesView, PresentationOptions};

pub(super) fn build(
    report: &Report,
    baseline: &BaselineScan,
    options: PresentationOptions,
) -> Option<FilesView> {
    if !options.verbose {
        return None;
    }

    let phases = report
        .predicate
        .as_ref()
        .map(|predicate| predicate.phases.as_slice())
        .unwrap_or_default();

    let entries = baseline
        .files
        .iter()
        .map(|file| {
            let file_stats = baseline.stats.get(&file.path);

            // ScanFile::num_records is not always populated, for example
            // when statistics came from stats_parsed rather than JSON stats.
            // Fall back to the parsed baseline statistics.
            let num_records = file
                .num_records
                .or_else(|| file_stats.and_then(|stats| stats.num_records));

            let mut stats = file_stats
                .map(|stats| {
                    stats
                        .columns
                        .iter()
                        .map(|(column, stats)| ColumnStatsView {
                            column: column.clone(),
                            min: stats.min.clone(),
                            max: stats.max.clone(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();

            // HashMap iteration order is intentionally unspecified.
            // Sorting here gives both render strategies deterministic input.
            stats.sort_by(|left, right| left.column.cmp(&right.column));

            let phase_states = phases
                .iter()
                .enumerate()
                .map(|(index, phase)| {
                    let candidate = if index == 0 {
                        true
                    } else {
                        phases[index - 1].surviving_paths.contains(&file.path)
                    };

                    if !candidate {
                        FilePhaseState::NotCandidate
                    } else if phase.surviving_paths.contains(&file.path) {
                        FilePhaseState::Kept
                    } else {
                        FilePhaseState::Dropped
                    }
                })
                .collect::<Vec<_>>();

            let pruned_by = phase_states
                .iter()
                .position(|state| *state == FilePhaseState::Dropped)
                .map(|index| phase_name(phases[index].kind));

            FileView {
                path: file.path.clone(),

                size_bytes: file.size,

                partition_values: file.partition_values.clone(),

                num_records,

                has_stats: file_stats.is_some(),

                stats,

                kept: pruned_by.is_none(),

                pruned_by,

                phase_states,
            }
        })
        .collect();

    Some(FilesView {
        entries,
        limit: options.limit,
    })
}
