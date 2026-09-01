use delta_kernel::engine_data::{FilteredRowVisitor, TypedGetData};
use delta_kernel::expressions::ColumnName;
use delta_kernel::scan::{ScanBuilder, StatsOptions, state::ScanFile};
use delta_kernel::schema::DataType;
use delta_kernel::{DeltaResult, Engine, GetData, RowIndexIterator, Snapshot};
use std::{
    collections::HashMap,
    sync::{Arc, LazyLock},
};

use crate::error::Result;
use crate::metadata::stats::{FileStats, parse_stats_json};

#[derive(Clone, Debug)]
pub struct FileInfo {
    pub path: String,
    pub size: i64,
    pub partition_values: HashMap<String, String>,
    pub num_records: Option<u64>,
    pub has_deletion_vector: bool,
}

pub struct BaselineScan {
    pub files: Vec<FileInfo>,
    pub stats: HashMap<String, FileStats>,
}

pub fn scan_baseline(snapshot: Arc<Snapshot>, engine: &dyn Engine) -> Result<BaselineScan> {
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

pub fn partition_columns_from_files(files: &[FileInfo]) -> Vec<String> {
    let mut columns: Vec<String> = files
        .iter()
        .flat_map(|f| f.partition_values.keys().cloned())
        .collect();
    columns.sort();
    columns.dedup();
    columns
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
