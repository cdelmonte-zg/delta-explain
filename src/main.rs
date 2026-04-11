mod predicate;

use std::collections::HashMap;
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::expressions::Predicate;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::scan::ScanBuilder;
use delta_kernel::{DeltaResult, Engine, Snapshot};
use num_format::{Locale, ToFormattedString};
use url::Url;

#[derive(Parser)]
#[command(name = "delta-explain", about = "Show step-by-step how Delta Lake prunes files given a predicate")]
struct Cli {
    /// Path to the Delta table (local path or URL)
    path: String,

    /// Predicate expression (e.g. "age > 30 AND country = 'DE'")
    #[arg(short, long)]
    predicate: Option<String>,
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
    if let Ok(url) = Url::parse(path) {
        if url.scheme() != "file" && url.has_host() {
            return Ok(url);
        }
    }
    let absolute = std::fs::canonicalize(path)
        .map_err(|e| delta_kernel::Error::Generic(format!("Invalid path '{path}': {e}")))?;
    Url::from_directory_path(&absolute)
        .map_err(|_| delta_kernel::Error::Generic(format!("Cannot convert path to URL: {absolute:?}")))
}

fn build_engine(url: &Url) -> DeltaResult<impl Engine> {
    let opts: HashMap<String, String> = HashMap::new();
    let store = store_from_url_opts(url, opts)?;
    Ok(DefaultEngineBuilder::<TokioBackgroundExecutor>::new(store).build())
}

fn count_scan_files(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
    predicate: Option<&Predicate>,
) -> DeltaResult<usize> {
    let mut builder = ScanBuilder::new(snapshot);
    if let Some(pred) = predicate {
        builder = builder.with_predicate(Arc::new(pred.clone()));
    }
    let scan = builder.build()?;
    let mut count = 0usize;
    for res in scan.scan_metadata(engine)? {
        let scan_meta = res?;
        count = scan_meta.visit_scan_files(count, |count: &mut usize, _file: ScanFile| {
            *count += 1;
        })?;
    }
    Ok(count)
}

/// Get partition column names by inspecting partition_values of scan files.
fn get_partition_columns(
    snapshot: Arc<Snapshot>,
    engine: &dyn Engine,
) -> DeltaResult<Vec<String>> {
    let scan = ScanBuilder::new(snapshot).build()?;
    for res in scan.scan_metadata(engine)? {
        let scan_meta = res?;
        let columns = scan_meta.visit_scan_files(
            Vec::<String>::new(),
            |cols: &mut Vec<String>, file: ScanFile| {
                if cols.is_empty() {
                    *cols = file.partition_values.keys().cloned().collect();
                    cols.sort();
                }
            },
        )?;
        if !columns.is_empty() {
            return Ok(columns);
        }
    }
    Ok(Vec::new())
}

fn fmt(n: usize) -> String {
    n.to_formatted_string(&Locale::en)
}

fn try_main() -> DeltaResult<()> {
    let cli = Cli::parse();

    let url = parse_table_uri(&cli.path)?;
    let engine = build_engine(&url)?;
    let snapshot = Snapshot::builder_for(url).build(&engine)?;

    let schema = snapshot.schema();
    let partition_columns = get_partition_columns(snapshot.clone(), &engine)?;

    // Total files (no predicate)
    let total_files = count_scan_files(snapshot.clone(), &engine, None)?;

    println!("Delta table: {}", cli.path);
    if let Some(ref pred_str) = cli.predicate {
        println!("Predicate: {pred_str}");
    }
    println!();
    println!("Files in snapshot: {}", fmt(total_files));

    if let Some(ref pred_str) = cli.predicate {
        let (part_preds, stats_preds) =
            predicate::split_predicate(pred_str, &schema, &partition_columns)
                .map_err(delta_kernel::Error::Generic)?;

        // Partition pruning step
        if !part_preds.is_empty() {
            let part_pred = if part_preds.len() == 1 {
                part_preds.into_iter().next().unwrap()
            } else {
                Predicate::and_from(part_preds)
            };

            let after_partition = count_scan_files(snapshot.clone(), &engine, Some(&part_pred))?;

            let part_display = extract_clauses(pred_str, |col| {
                partition_columns.contains(&col.to_string())
            });
            println!();
            println!("Partition pruning");
            println!("  predicate: {part_display}");
            println!("  files remaining: {}", fmt(after_partition));

            // Data skipping step (full predicate)
            if !stats_preds.is_empty() {
                let full_pred = predicate::parse_predicate(pred_str, &schema)
                    .map_err(delta_kernel::Error::Generic)?;

                let after_skipping =
                    count_scan_files(snapshot.clone(), &engine, Some(&full_pred))?;

                let stats_display = extract_clauses(pred_str, |col| {
                    !partition_columns.contains(&col.to_string())
                });
                println!();
                println!("Data skipping (statistics)");
                println!("  predicate: {stats_display}");
                println!("  files remaining: {}", fmt(after_skipping));
            }
        } else {
            // No partition predicates, only data skipping
            let full_pred = predicate::parse_predicate(pred_str, &schema)
                .map_err(delta_kernel::Error::Generic)?;

            let after_skipping = count_scan_files(snapshot.clone(), &engine, Some(&full_pred))?;

            println!();
            println!("Data skipping (statistics)");
            println!("  predicate: {pred_str}");
            println!("  files remaining: {}", fmt(after_skipping));
        }
    }

    Ok(())
}

fn extract_clauses(pred_str: &str, keep: impl Fn(&str) -> bool) -> String {
    let upper = pred_str.to_uppercase();
    let mut parts = Vec::new();
    let mut start = 0;

    let mut indices = Vec::new();
    let mut s = 0;
    while let Some(pos) = upper[s..].find(" AND ") {
        indices.push(s + pos);
        s = s + pos + 5;
    }

    let mut clauses = Vec::new();
    for &idx in &indices {
        clauses.push(&pred_str[start..idx]);
        start = idx + 5;
    }
    clauses.push(&pred_str[start..]);

    let ops = ["!=", "<=", ">=", "=", "<", ">"];
    for clause in clauses {
        let clause = clause.trim();
        for op in ops {
            if let Some(idx) = clause.find(op) {
                let col = clause[..idx].trim();
                if keep(col) {
                    parts.push(clause.to_string());
                }
                break;
            }
        }
    }

    parts.join(" AND ")
}
