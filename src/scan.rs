//! Kernel-backed metadata scans: file enumeration and per-file statistics.
//!
//! Everything here goes through the kernel's metadata path (`scan_metadata`),
//! never the data path that opens Parquet footers. The kernel's log replay
//! merges JSON commits with checkpoint Parquet, so the results also cover
//! files whose `add` action survives only inside a checkpoint.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use delta_kernel::engine_data::{FilteredRowVisitor, GetData, RowIndexIterator, TypedGetData};
use delta_kernel::expressions::{ColumnName, Predicate};
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::{ScanBuilder, StatsOptions};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Engine, Snapshot};

use crate::error::Result;
use crate::report::FileInfo;
use crate::stats::{FileStats, parse_stats_json};

/// Everything the baseline (predicate-less) scan yields in one log replay:
/// the snapshot's file listing and the per-file statistics map.
pub struct BaselineScan {
    pub files: Vec<FileInfo>,
    pub stats: HashMap<String, FileStats>,
}

/// Derive partition columns from the `partitionValues` keys of the baseline
/// scan's files.
///
/// Per the Delta protocol every `add` action of a partitioned table carries
/// its partition values keyed by partition column, and the kernel returns
/// them from its full log replay, checkpoint Parquet included. This is
/// protocol data, not an inference from directory names. Used as a fallback
/// when no `metaData` action survives in the JSON commits (a fully
/// checkpointed log after retention cleanup); the `metaData` action stays the
/// primary source because it is authoritative and also covers empty tables.
/// The residual blind spot is the intersection of the two: an empty
/// partitioned table whose log is fully checkpointed has no files to derive
/// from, and its partition columns stay undetected.
pub fn partition_columns_from_files(files: &[FileInfo]) -> Vec<String> {
    let mut columns: Vec<String> = files
        .iter()
        .flat_map(|f| f.partition_values.keys().cloned())
        .collect();
    columns.sort();
    columns.dedup();
    columns
}

/// Run the baseline scan once, collecting the file listing and the per-file
/// statistics from the same `scan_metadata` pass, so the log is replayed a
/// single time. The stats come from the `stats` JSON string the kernel
/// carries on each scan row. Files whose Add action carries no `stats`
/// payload get no entry; the report layer treats their absence as "no stats".
pub fn scan_baseline(snapshot: Arc<Snapshot>, engine: &dyn Engine) -> Result<BaselineScan> {
    // StatsOptions::all() requests the full struct stats schema, which is
    // what makes the kernel read stats_parsed from checkpoint Parquet and
    // populate the scan row's `stats` field via COALESCE(add.stats,
    // ToJson(add.stats_parsed)). The predicate-less baseline reads no stats
    // columns otherwise (json_only trims the stats schema to predicate
    // references), so a checkpoint written with
    // delta.checkpoint.writeStatsAsJson=false (structured stats_parsed only,
    // no JSON stats) would come back with no stats at all.
    let scan = ScanBuilder::new(snapshot)
        .with_stats(StatsOptions::all())
        .build()?;
    let mut files = Vec::new();
    let mut visitor = StatsVisitor {
        stats: HashMap::new(),
    };
    for res in scan.scan_metadata(engine)? {
        let scan_meta = res?;
        files = scan_meta.visit_scan_files(files, push_file_info)?;
        visitor.visit_rows_of(&scan_meta.scan_files)?;
    }
    Ok(BaselineScan {
        files,
        stats: visitor.stats,
    })
}

/// Collect the files surviving a metadata scan with the given predicate.
pub fn collect_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    predicate: Option<&Predicate>,
) -> Result<Vec<FileInfo>> {
    let mut builder = ScanBuilder::new(snapshot);
    if let Some(pred) = predicate {
        builder = builder.with_predicate(Arc::new(pred.clone()));
    }
    let scan = builder.build()?;
    let mut files = Vec::new();
    for res in scan.scan_metadata(engine)? {
        let scan_meta = res?;
        files = scan_meta.visit_scan_files(files, push_file_info)?;
    }
    Ok(files)
}

fn push_file_info(files: &mut Vec<FileInfo>, file: ScanFile) {
    files.push(FileInfo {
        path: file.path.clone(),
        size: file.size,
        partition_values: file.partition_values.clone(),
        num_records: file.stats.map(|s| s.num_records),
        has_deletion_vector: file.dv_info.has_vector(),
    });
}

struct StatsVisitor {
    stats: HashMap<String, FileStats>,
}

static STATS_COLUMNS: LazyLock<(Vec<ColumnName>, Vec<DataType>)> = LazyLock::new(|| {
    (
        vec![ColumnName::new(["path"]), ColumnName::new(["stats"])],
        vec![DataType::STRING, DataType::STRING],
    )
});

impl FilteredRowVisitor for StatsVisitor {
    fn selected_column_names_and_types(&self) -> (&'static [ColumnName], &'static [DataType]) {
        (&STATS_COLUMNS.0, &STATS_COLUMNS.1)
    }

    fn visit_filtered<'a>(
        &mut self,
        getters: &[&'a dyn GetData<'a>],
        rows: RowIndexIterator<'_>,
    ) -> DeltaResult<()> {
        // The getter arity mirrors selected_column_names_and_types; if the
        // kernel ever hands us fewer, fail the scan instead of panicking.
        let (path_getter, stats_getter) = match getters {
            [path, stats, ..] => (path, stats),
            _ => {
                return Err(delta_kernel::Error::generic(
                    "scan row visitor received fewer getters than requested columns",
                ));
            }
        };
        for row_index in rows {
            let path: Option<String> = path_getter.get_opt(row_index, "scanFile.path")?;
            let Some(path) = path else {
                continue;
            };
            let stats_str: Option<String> = stats_getter.get_opt(row_index, "scanFile.stats")?;
            if let Some(parsed) = stats_str.as_deref().and_then(parse_stats_json) {
                self.stats.insert(path, parsed);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, parts: &[(&str, &str)]) -> FileInfo {
        FileInfo {
            path: path.into(),
            size: 1,
            partition_values: parts
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            num_records: None,
            has_deletion_vector: false,
        }
    }

    #[test]
    fn partition_columns_are_deduped_and_sorted() {
        let files = vec![
            file("a", &[("country", "DE"), ("year", "2026")]),
            file("b", &[("year", "2025"), ("country", "IT")]),
        ];
        assert_eq!(
            partition_columns_from_files(&files),
            vec!["country".to_string(), "year".to_string()]
        );
    }

    #[test]
    fn unpartitioned_files_yield_no_columns() {
        let files = vec![file("a", &[]), file("b", &[])];
        assert!(partition_columns_from_files(&files).is_empty());
    }
}
