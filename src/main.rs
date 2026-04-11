mod predicate;
mod report;
mod stats;

use std::collections::{HashMap, HashSet};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::expressions::Predicate;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use object_store::DynObjectStore;
use url::Url;

use report::{FileInfo, OutputFormat, PhaseResult, PruningReport};

#[derive(Parser)]
#[command(name = "delta-explain", about = "Make Delta pruning visible")]
#[command(after_help = "\
Examples:
  Diagnostic (local):
    delta-explain ./my-table -w \"country = 'DE'\"
    delta-explain ./my-table -w \"age > 30\" --verbose

  CI assertion:
    delta-explain ./my-table -w \"country = 'DE'\" --min-pruning 60
    delta-explain ./my-table --assert-stats
    delta-explain ./my-table -w \"age > 30\" --format json

  Cloud:
    delta-explain --env-creds s3://bucket/table -w \"age > 30\"
    delta-explain --region us-east-1 --public s3://bucket/table -w \"id = 42\"
")]
struct Cli {
    /// Path to the Delta table (local path, s3://, az://, gs://)
    path: String,

    /// Predicate expression (e.g. "age > 30 AND country = 'DE'")
    #[arg(short = 'w', long = "where")]
    predicate: Option<String>,

    /// Show per-file details (kept/dropped with reason)
    #[arg(short, long)]
    verbose: bool,

    // ── CI / assertion flags ────────────────────────────────────────
    /// Output format: "text" (default) or "json"
    #[arg(long, default_value = "text")]
    format: String,

    /// Fail (exit 1) if total pruning percentage is below this threshold.
    /// Requires --where.
    #[arg(long, value_name = "PERCENT")]
    min_pruning: Option<f64>,

    /// Fail (exit 1) if any file in the snapshot is missing statistics.
    #[arg(long)]
    assert_stats: bool,

    // ── Cloud storage flags ─────────────────────────────────────────
    /// AWS region (S3 only)
    #[arg(long)]
    region: Option<String>,

    /// Key=value options for the object store backend. Can be repeated.
    #[arg(long = "option", value_name = "KEY=VALUE")]
    options: Vec<String>,

    /// Get cloud credentials from environment variables
    #[arg(long)]
    env_creds: bool,

    /// Access a public bucket (S3: skip signature)
    #[arg(long)]
    public: bool,
}

fn main() -> ExitCode {
    match try_main() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("Error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

fn parse_table_uri(path: &str) -> DeltaResult<Url> {
    if let Ok(url) = Url::parse(path)
        && url.scheme() != "file"
        && url.has_host()
    {
        return Ok(url);
    }
    let absolute = std::fs::canonicalize(path)
        .map_err(|e| delta_kernel::Error::Generic(format!("Invalid path '{path}': {e}")))?;
    Url::from_directory_path(&absolute).map_err(|_| {
        delta_kernel::Error::Generic(format!("Cannot convert path to URL: {absolute:?}"))
    })
}

struct EngineAndStore {
    engine: Box<dyn Engine>,
    store: Arc<DynObjectStore>,
}

fn build_engine(url: &Url, cli: &Cli) -> DeltaResult<EngineAndStore> {
    let mut opts: HashMap<String, String> = HashMap::new();

    if let Some(ref region) = cli.region {
        opts.insert("region".into(), region.clone());
    }
    if cli.public {
        opts.insert("skip_signature".into(), "true".into());
    }
    for option in &cli.options {
        let (key, value) = option.split_once('=').ok_or_else(|| {
            delta_kernel::Error::Generic(format!(
                "Invalid option format '{option}', expected KEY=VALUE"
            ))
        })?;
        opts.insert(key.to_ascii_lowercase(), value.into());
    }

    let store = store_from_url_opts(url, opts)?;
    let engine = DefaultEngineBuilder::<TokioBackgroundExecutor>::new(store.clone()).build();

    Ok(EngineAndStore {
        engine: Box::new(engine),
        store,
    })
}

fn collect_files(
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
        files =
            scan_meta.visit_scan_files(files, |files: &mut Vec<FileInfo>, file: ScanFile| {
                files.push(FileInfo {
                    path: file.path.clone(),
                    size: file.size,
                    partition_values: file.partition_values.clone(),
                    num_records: file.stats.map(|s| s.num_records),
                });
            })?;
    }
    Ok(files)
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    let output_format = match cli.format.as_str() {
        "json" => OutputFormat::Json,
        _ => OutputFormat::Text,
    };

    let url = parse_table_uri(&cli.path)?;
    let EngineAndStore { engine, store } = build_engine(&url, &cli)?;
    let snapshot = Snapshot::builder_for(url.clone()).build(engine.as_ref())?;
    let schema = snapshot.schema();

    let all_files = collect_files(snapshot.clone(), engine.as_ref(), None)?;
    let partition_columns: Vec<String> = all_files
        .first()
        .map(|f| {
            let mut cols: Vec<String> = f.partition_values.keys().cloned().collect();
            cols.sort();
            cols
        })
        .unwrap_or_default();

    let file_stats = stats::read_stats_from_log(&url, &store)?;

    let mut report = PruningReport {
        table_path: cli.path.clone(),
        version: snapshot.version(),
        total_files: all_files.len(),
        all_files,
        file_stats,
        phases: Vec::new(),
    };

    if let Some(ref pred_str) = cli.predicate {
        let (part_preds, stats_preds) =
            predicate::split_predicate(pred_str, &schema, &partition_columns)
                .map_err(delta_kernel::Error::Generic)?;

        if !part_preds.is_empty() {
            let part_pred = if part_preds.len() == 1 {
                part_preds.into_iter().next().unwrap()
            } else {
                Predicate::and_from(part_preds)
            };

            let surviving = collect_files(snapshot.clone(), engine.as_ref(), Some(&part_pred))?;
            let surviving_paths: HashSet<String> =
                surviving.iter().map(|f| f.path.clone()).collect();

            report.phases.push(PhaseResult {
                name: "Partition pruning".into(),
                predicate_display: predicate::extract_clauses(pred_str, |col| {
                    partition_columns.contains(&col.to_string())
                }),
                input_count: report.total_files,
                output_count: surviving.len(),
                surviving_paths,
            });

            if !stats_preds.is_empty() {
                let full_pred = predicate::parse_predicate(pred_str, &schema)
                    .map_err(delta_kernel::Error::Generic)?;

                let prev_count = surviving.len();
                let surviving = collect_files(snapshot.clone(), engine.as_ref(), Some(&full_pred))?;
                let surviving_paths: HashSet<String> =
                    surviving.iter().map(|f| f.path.clone()).collect();

                report.phases.push(PhaseResult {
                    name: "Data skipping (min/max statistics)".into(),
                    predicate_display: predicate::extract_clauses(pred_str, |col| {
                        !partition_columns.contains(&col.to_string())
                    }),
                    input_count: prev_count,
                    output_count: surviving.len(),
                    surviving_paths,
                });
            }
        } else {
            let full_pred = predicate::parse_predicate(pred_str, &schema)
                .map_err(delta_kernel::Error::Generic)?;

            let surviving = collect_files(snapshot.clone(), engine.as_ref(), Some(&full_pred))?;
            let surviving_paths: HashSet<String> =
                surviving.iter().map(|f| f.path.clone()).collect();

            report.phases.push(PhaseResult {
                name: "Data skipping (min/max statistics)".into(),
                predicate_display: pred_str.clone(),
                input_count: report.total_files,
                output_count: surviving.len(),
                surviving_paths,
            });
        }
    }

    // ── Output ──────────────────────────────────────────────────────

    match output_format {
        OutputFormat::Text => report.print_text(cli.verbose, cli.predicate.as_deref()),
        OutputFormat::Json => report.print_json(cli.predicate.as_deref()),
    }

    // ── Assertions (CI mode) ────────────────────────────────────────

    let mut failed = false;

    if let Some(threshold) = cli.min_pruning {
        let actual = report.total_pruning_pct();
        if actual < threshold {
            eprintln!(
                "ASSERTION FAILED: total pruning {actual:.1}% is below threshold {threshold:.1}%"
            );
            failed = true;
        }
    }

    if cli.assert_stats {
        let missing: Vec<&str> = report
            .all_files
            .iter()
            .filter(|f| !report.file_stats.contains_key(&f.path))
            .map(|f| f.path.as_str())
            .collect();
        if !missing.is_empty() {
            eprintln!(
                "ASSERTION FAILED: {} file(s) missing statistics:",
                missing.len()
            );
            for path in &missing {
                eprintln!("  {path}");
            }
            failed = true;
        }
    }

    if failed {
        std::process::exit(1);
    }

    Ok(())
}
