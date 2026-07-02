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
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Engine, Snapshot};

use crate::report::FileInfo;
use crate::stats::{FileStats, parse_stats_json};

/// Everything the baseline (predicate-less) scan yields in one log replay:
/// the snapshot's file listing and the per-file statistics map.
pub struct BaselineScan {
    pub files: Vec<FileInfo>,
    pub stats: HashMap<String, FileStats>,
}

/// Run the baseline scan once, collecting the file listing and the per-file
/// statistics from the same `scan_metadata` pass, so the log is replayed a
/// single time. The stats come from the `stats` JSON string the kernel
/// carries on each scan row. Files whose Add action carries no `stats`
/// payload get no entry; the report layer treats their absence as "no stats".
pub fn scan_baseline(snapshot: Arc<Snapshot>, engine: &dyn Engine) -> DeltaResult<BaselineScan> {
    // include_all_stats_columns() requests the parsed stats schema, which is
    // what makes the kernel populate the scan row's `stats` field via
    // COALESCE(add.stats, ToJson(add.stats_parsed)). Without it, a checkpoint
    // written with delta.checkpoint.writeStatsAsJson=false (structured
    // stats_parsed only, no JSON stats) would come back with no stats at all.
    let scan = ScanBuilder::new(snapshot)
        .include_all_stats_columns()
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
) -> DeltaResult<Vec<FileInfo>> {
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
        for row_index in rows {
            let path: Option<String> = getters[0].get_opt(row_index, "scanFile.path")?;
            let Some(path) = path else {
                continue;
            };
            let stats_str: Option<String> = getters[1].get_opt(row_index, "scanFile.stats")?;
            if let Some(parsed) = stats_str.as_deref().and_then(parse_stats_json) {
                self.stats.insert(path, parsed);
            }
        }
        Ok(())
    }
}
