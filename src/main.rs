use std::collections::{HashMap, HashSet};
use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use delta_kernel::engine::default::DefaultEngineBuilder;
use delta_kernel::engine::default::storage::store_from_url_opts;
use delta_kernel::{Engine, Snapshot};
use object_store::DynObjectStore;
use url::Url;

use delta_explain::error::{Error, Result};
use delta_explain::render::OutputFormat;
use delta_explain::report::{OverallResult, PruningReport};
use delta_explain::{
    attribution, credentials, features, gates, kernel_bridge, predicate_analyzer, predicate_ast,
    render, scan, stats,
};

#[derive(Parser)]
#[command(name = "delta-explain", version, about = "Make Delta pruning visible")]
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

  Time travel:
    delta-explain ./my-table -w \"age > 30\" --at-version 3
")]
struct Cli {
    /// Path to the Delta table (local path, s3://, az://, gs://)
    path: String,

    /// Predicate expression (e.g. "age > 30 AND country = 'DE'")
    #[arg(short = 'w', long = "where")]
    predicate: Option<String>,

    /// Show per-file details (kept/dropped with reason); in JSON, adds
    /// the "files" array
    #[arg(short, long)]
    verbose: bool,

    /// Cap per-file listings at N entries (text phases and JSON "files").
    /// Only meaningful together with --verbose.
    #[arg(long, value_name = "N")]
    limit: Option<usize>,

    // ── CI / assertion flags ────────────────────────────────────────
    /// Output format
    #[arg(long, default_value = "text", value_parser = ["text", "json"])]
    format: String,

    /// Fail (exit 1) if total pruning percentage is below this threshold.
    /// Requires --where.
    #[arg(long, value_name = "PERCENT", requires = "predicate")]
    min_pruning: Option<f64>,

    /// Fail (exit 1) if any file in the snapshot is missing statistics.
    #[arg(long)]
    assert_stats: bool,

    /// Analyze the table at this version instead of the latest (time travel).
    #[arg(long, value_name = "N")]
    at_version: Option<u64>,

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

    /// Resolve static AWS credentials from this profile in
    /// ~/.aws/credentials and ~/.aws/config (S3 only)
    #[arg(long, value_name = "NAME")]
    profile: Option<String>,

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

fn parse_table_uri(path: &str) -> Result<Url> {
    if let Ok(mut url) = Url::parse(path)
        && url.scheme() != "file"
        && url.has_host()
    {
        // The kernel locates the log via `url.join("_delta_log/")`. Per RFC 3986
        // that replaces the last path segment unless the base ends in a slash,
        // so without this `s3://bucket/prefix/table` would resolve the log at
        // the bucket root ("No files in log segment"). Local paths are immune
        // because `Url::from_directory_path` already appends a trailing slash.
        if !url.path().ends_with('/') {
            let with_slash = format!("{}/", url.path());
            url.set_path(&with_slash);
        }
        return Ok(url);
    }
    let absolute = std::fs::canonicalize(path)
        .map_err(|e| Error::TableUri(format!("Invalid path '{path}': {e}")))?;
    Url::from_directory_path(&absolute)
        .map_err(|_| Error::TableUri(format!("Cannot convert path to URL: {absolute:?}")))
}

struct EngineAndStore {
    engine: Box<dyn Engine>,
    store: Arc<DynObjectStore>,
}

fn build_engine(url: &Url, cli: &Cli) -> Result<EngineAndStore> {
    let mut opts: HashMap<String, String> = HashMap::new();

    // Profile values go in first so the explicit flags below can override.
    if let Some(ref profile) = cli.profile {
        opts.extend(credentials::resolve_aws_profile(profile)?);
    }
    if let Some(ref region) = cli.region {
        opts.insert("region".into(), region.clone());
    }
    if cli.public {
        opts.insert("skip_signature".into(), "true".into());
    }
    if cli.env_creds {
        opts.insert("allow_env".into(), "true".into());
    }
    for option in &cli.options {
        let (key, value) = option.split_once('=').ok_or_else(|| {
            Error::Options(format!(
                "Invalid option format '{option}', expected KEY=VALUE"
            ))
        })?;
        opts.insert(key.to_ascii_lowercase(), value.into());
    }

    let store = store_from_url_opts(url, opts)?;
    let engine = DefaultEngineBuilder::new(store.clone()).build();

    Ok(EngineAndStore {
        engine: Box::new(engine),
        store,
    })
}

fn try_main() -> Result<()> {
    let cli = Cli::parse();
    let start = std::time::Instant::now();

    let output_format = match cli.format.as_str() {
        "json" => OutputFormat::Json,
        _ => OutputFormat::Text,
    };

    let url = parse_table_uri(&cli.path)?;
    let EngineAndStore { engine, store } = build_engine(&url, &cli)?;
    let mut snapshot_builder = Snapshot::builder_for(url.clone());
    if let Some(version) = cli.at_version {
        snapshot_builder = snapshot_builder.at_version(version);
    }
    let snapshot = snapshot_builder.build(engine.as_ref())?;
    let schema = snapshot.schema();

    let scan::BaselineScan {
        files: all_files,
        stats: file_stats,
    } = scan::scan_baseline(snapshot.clone(), engine.as_ref())?;
    let log_metadata = stats::read_log_metadata(&url, &store)?;
    let mut partition_columns = log_metadata.partition_columns;
    if partition_columns.is_empty() {
        // On a fully checkpointed log no metaData action survives in the JSON
        // commits; fall back to the partitionValues keys the kernel replayed.
        partition_columns = scan::partition_columns_from_files(&all_files);
    }

    let table_features = features::detect(
        &snapshot,
        &all_files,
        log_metadata.clustering_domain.as_deref(),
    );

    let mut report = PruningReport {
        analysis: None,
        table_features,
        table_path: cli.path.clone(),
        version: snapshot.version(),
        total_files: all_files.len(),
        all_files,
        file_stats,
        phases: Vec::new(),
        elapsed_ms: 0,
        assertions: Vec::new(),
        overall_result: None,
    };

    if let Some(ref pred_str) = cli.predicate {
        let pred_ast = predicate_ast::parse(pred_str)?.normalized();
        let classified = predicate_analyzer::classify(&pred_ast, &partition_columns);
        let analysis = classified.analysis;

        let partition_survivors = match &classified.partition_pred {
            Some(part_ast) => {
                let part_pred = kernel_bridge::emit_predicate(part_ast, &schema)?;
                let surviving =
                    scan::collect_files(snapshot.clone(), engine.as_ref(), Some(&part_pred))?;
                Some(
                    surviving
                        .into_iter()
                        .map(|f| f.path)
                        .collect::<HashSet<String>>(),
                )
            }
            None => None,
        };

        let full_survivors = if analysis.stats_safe.is_some() || analysis.unsplittable.is_some() {
            // Unsupported fragments degrade instead of failing: scan with
            // the predicate stripped of them (conservative, keeps more
            // files), or with no predicate at all when nothing survives
            // the strip. The analysis notes explain the gap to the user.
            let surviving = match pred_ast.without_unsupported() {
                Some(scan_pred) => {
                    let full_pred = kernel_bridge::emit_predicate(&scan_pred, &schema)?;
                    scan::collect_files(snapshot.clone(), engine.as_ref(), Some(&full_pred))?
                }
                None => scan::collect_files(snapshot.clone(), engine.as_ref(), None)?,
            };
            Some(
                surviving
                    .into_iter()
                    .map(|f| f.path)
                    .collect::<HashSet<String>>(),
            )
        } else {
            None
        };

        report.phases = attribution::build_phases(
            &analysis,
            report.total_files,
            partition_survivors,
            full_survivors,
        );
        report.analysis = Some(analysis);
    }

    // ── Assertions (CI mode) ────────────────────────────────────────

    let outcome = gates::evaluate(&report, cli.min_pruning, cli.assert_stats);
    for failure in &outcome.failures {
        eprintln!("{failure}");
    }
    report.assertions = outcome.assertions;
    report.overall_result = outcome.overall;

    report.elapsed_ms = start.elapsed().as_millis();

    // ── Output ──────────────────────────────────────────────────────

    match output_format {
        OutputFormat::Text => {
            render::print_text(&report, cli.verbose, cli.limit, cli.predicate.as_deref())
        }
        OutputFormat::Json => {
            render::print_json(&report, cli.verbose, cli.limit, cli.predicate.as_deref())
        }
    }

    if matches!(report.overall_result, Some(OverallResult::Fail)) {
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_table_uri;

    #[test]
    fn s3_uri_gets_trailing_slash_so_log_join_appends() {
        let url = parse_table_uri("s3://bucket/prefix/table").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/prefix/table/");
        // This is exactly how the kernel locates the transaction log:
        assert_eq!(
            url.join("_delta_log/").unwrap().as_str(),
            "s3://bucket/prefix/table/_delta_log/"
        );
    }

    #[test]
    fn s3_uri_with_trailing_slash_is_unchanged() {
        let url = parse_table_uri("s3://bucket/prefix/table/").unwrap();
        assert_eq!(url.as_str(), "s3://bucket/prefix/table/");
    }

    #[test]
    fn s3_bucket_root_resolves_log_at_root() {
        let url = parse_table_uri("s3://bucket").unwrap();
        assert_eq!(
            url.join("_delta_log/").unwrap().as_str(),
            "s3://bucket/_delta_log/"
        );
    }

    #[test]
    fn local_path_is_a_directory_url() {
        // Relative existing dir (crate root) -> file:// URL with trailing slash.
        let url = parse_table_uri(".").unwrap();
        assert_eq!(url.scheme(), "file");
        assert!(url.path().ends_with('/'));
    }
}
