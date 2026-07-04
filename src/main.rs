// Same production-code rule as the library crate: panics are enforced away
// by the compiler, not by review discipline (unit tests are exempt).
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable
    )
)]

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
    attribution, credentials, debug_dump, features, gates, kernel_bridge, predicate_analyzer,
    predicate_ast, render, scan, stats,
};

#[derive(Parser)]
#[command(name = "delta-explain", version, about = "Make Delta pruning visible")]
#[command(after_help = "\
Examples:
  Diagnostic (local):
    delta-explain ./my-table -w \"country = 'DE'\"
    delta-explain ./my-table -w \"age > 30\" --verbose
    delta-explain ./my-table -w \"age > 30\" --debug-ir ir.txt

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

    /// Write this run's intermediate representations (predicate AST before
    /// and after normalization, classification, lowered kernel predicates,
    /// survivor counts, kernel trace) to FILE. Diagnostic output: the
    /// format is unstable and outside the CLI/JSON contract.
    #[arg(long = "debug-ir", value_name = "FILE")]
    debug_ir: Option<String>,

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

/// The environment variables `--env-creds` reads, mapped to object_store
/// option keys. `object_store::parse_url_opts` ignores keys a backend does
/// not know, so the mapping can be cloud-agnostic; the later entry wins
/// when two variables target the same key (AWS_REGION over
/// AWS_DEFAULT_REGION).
const ENV_CREDENTIAL_MAP: &[(&str, &str)] = &[
    ("AWS_DEFAULT_REGION", "region"),
    ("AWS_REGION", "region"),
    ("AWS_ACCESS_KEY_ID", "access_key_id"),
    ("AWS_SECRET_ACCESS_KEY", "secret_access_key"),
    ("AWS_SESSION_TOKEN", "session_token"),
    ("AWS_ENDPOINT_URL", "endpoint"),
    ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),
    ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),
    ("GOOGLE_SERVICE_ACCOUNT", "google_service_account"),
    (
        "GOOGLE_APPLICATION_CREDENTIALS",
        "google_application_credentials",
    ),
];

fn env_credential_options(get: impl Fn(&str) -> Option<String>) -> Vec<(String, String)> {
    ENV_CREDENTIAL_MAP
        .iter()
        .filter_map(|(var, key)| get(var).map(|v| (key.to_string(), v)))
        .collect()
}

fn build_engine(url: &Url, cli: &Cli) -> Result<EngineAndStore> {
    let mut opts: HashMap<String, String> = HashMap::new();

    // Layering, least to most explicit: environment, then profile, then
    // flags, then --option. Whatever is inserted later wins.
    if cli.env_creds {
        // "allow_env" used to be passed to the store here, but no layer
        // ever read it: object_store's parse path silently drops unknown
        // keys and never consults the environment, so --env-creds was a
        // no-op that fell through to the instance-metadata chain. The
        // variables are now read here and injected as explicit options.
        opts.extend(env_credential_options(|var| std::env::var(var).ok()));
    }
    if let Some(ref profile) = cli.profile {
        opts.extend(credentials::resolve_aws_profile(profile)?);
    }
    if let Some(ref region) = cli.region {
        opts.insert("region".into(), region.clone());
    }
    if cli.public {
        opts.insert("skip_signature".into(), "true".into());
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

    // Created before the first kernel call so the trace capture sees the
    // whole log replay, not just the phase scans.
    let mut debug_dump = match cli.debug_ir.as_deref() {
        Some(path) => {
            let mut dump = debug_dump::DebugDump::create(path)?;
            dump.section(
                "invocation",
                &format!(
                    "table: {}\npredicate: {}",
                    cli.path,
                    cli.predicate.as_deref().unwrap_or("(none)")
                ),
            )?;
            Some(dump)
        }
        None => None,
    };

    // Read the log's own metadata before asking the kernel for a snapshot:
    // a catalog-managed table deserves this tool's explanation, not the
    // kernel's API-flavored refusal.
    let log_metadata = stats::read_log_metadata(&url, &store)?;
    if let Some(feature) = features::catalog_managed_feature(&log_metadata.reader_features) {
        return Err(Error::UnsupportedTable(format!(
            "table is catalog-managed (reader feature '{feature}'): its latest \
             commits live in the catalog, not the filesystem log, so a \
             filesystem-only analysis cannot be trusted. delta-explain does \
             not support catalog-managed tables yet"
        )));
    }

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
    let mut partition_columns = log_metadata.partition_columns;
    if partition_columns.is_empty() {
        // On a fully checkpointed log no metaData action survives in the JSON
        // commits; fall back to the partitionValues keys the kernel replayed.
        partition_columns = scan::partition_columns_from_files(&all_files);
    }

    if let Some(dump) = debug_dump.as_mut() {
        dump.section(
            "snapshot",
            &format!(
                "version: {}\nfiles in snapshot: {}\npartition columns: {:?}",
                snapshot.version(),
                all_files.len(),
                partition_columns
            ),
        )?;
    }

    let table_features = features::detect(
        &snapshot,
        &all_files,
        log_metadata.clustering_domain.as_deref(),
        &log_metadata.writer_features,
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
        let parsed = predicate_ast::parse(pred_str)?;
        if let Some(dump) = debug_dump.as_mut() {
            dump.section(
                "owned AST (parsed)",
                &format!("rendered: {parsed}\n\n{parsed:#?}"),
            )?;
        }
        let pred_ast = parsed.normalized();
        if let Some(dump) = debug_dump.as_mut() {
            dump.section(
                "owned AST (normalized)",
                &format!("rendered: {pred_ast}\n\n{pred_ast:#?}"),
            )?;
        }
        let classified = predicate_analyzer::classify(&pred_ast, &partition_columns);
        let analysis = classified.analysis;
        if let Some(dump) = debug_dump.as_mut() {
            dump.section("classification", &format!("{analysis:#?}"))?;
        }

        let partition_survivors = match &classified.partition_pred {
            Some(part_ast) => {
                let part_pred = kernel_bridge::emit_predicate(part_ast, &schema)?;
                if let Some(dump) = debug_dump.as_mut() {
                    dump.section(
                        "kernel predicate: partition-only scan",
                        &format!("lowered from: {part_ast}\n\n{part_pred:#?}"),
                    )?;
                }
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
                    if let Some(dump) = debug_dump.as_mut() {
                        dump.section(
                            "kernel predicate: full scan",
                            &format!(
                                "scan predicate after stripping unsupported fragments: \
                                 {scan_pred}\n\n{full_pred:#?}"
                            ),
                        )?;
                    }
                    scan::collect_files(snapshot.clone(), engine.as_ref(), Some(&full_pred))?
                }
                None => {
                    if let Some(dump) = debug_dump.as_mut() {
                        dump.section(
                            "kernel predicate: full scan",
                            "no fragment survives the strip; the full scan runs without \
                             a predicate",
                        )?;
                    }
                    scan::collect_files(snapshot.clone(), engine.as_ref(), None)?
                }
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

        if let Some(dump) = debug_dump.as_mut() {
            let partition_line = match &partition_survivors {
                Some(s) => format!("partition-only scan: {} files", s.len()),
                None => "partition-only scan: skipped (no partition-safe fragment)".to_string(),
            };
            let full_line = match &full_survivors {
                Some(s) => format!("full scan: {} files", s.len()),
                None => "full scan: skipped (pure-partition predicate)".to_string(),
            };
            dump.section(
                "survivor sets",
                &format!(
                    "baseline: {} files\n{partition_line}\n{full_line}",
                    report.total_files
                ),
            )?;
        }

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

    // All kernel work is done; close the dump (appends the captured kernel
    // trace) before rendering, so the file is complete even on the exit(1)
    // path of a failed gate.
    if let Some(dump) = debug_dump.take() {
        dump.finish()?;
    }

    // ── Output ──────────────────────────────────────────────────────

    match output_format {
        OutputFormat::Text => {
            render::print_text(&report, cli.verbose, cli.limit, cli.predicate.as_deref())
        }
        OutputFormat::Json => {
            render::print_json(&report, cli.verbose, cli.limit, cli.predicate.as_deref())?
        }
    }

    if matches!(report.overall_result, Some(OverallResult::Fail)) {
        std::process::exit(1);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{env_credential_options, parse_table_uri};

    #[test]
    fn env_creds_map_known_variables_to_store_options() {
        let fake = |var: &str| match var {
            "AWS_ACCESS_KEY_ID" => Some("AKIA123".to_string()),
            "AWS_SECRET_ACCESS_KEY" => Some("secret".to_string()),
            "AWS_REGION" => Some("eu-central-1".to_string()),
            "GOOGLE_APPLICATION_CREDENTIALS" => Some("/path/key.json".to_string()),
            _ => None,
        };
        let opts = env_credential_options(fake);
        assert!(opts.contains(&("access_key_id".into(), "AKIA123".into())));
        assert!(opts.contains(&("secret_access_key".into(), "secret".into())));
        assert!(opts.contains(&("region".into(), "eu-central-1".into())));
        assert!(opts.contains(&(
            "google_application_credentials".into(),
            "/path/key.json".into()
        )));
    }

    #[test]
    fn aws_region_wins_over_default_region() {
        let fake = |var: &str| match var {
            "AWS_DEFAULT_REGION" => Some("us-east-1".to_string()),
            "AWS_REGION" => Some("eu-central-1".to_string()),
            _ => None,
        };
        // Both map to "region"; insertion order makes AWS_REGION win when
        // the caller extends a map with these pairs in order.
        let opts = env_credential_options(fake);
        let regions: Vec<&(String, String)> = opts.iter().filter(|(k, _)| k == "region").collect();
        assert_eq!(
            regions.last().map(|(_, v)| v.as_str()),
            Some("eu-central-1")
        );
    }

    #[test]
    fn empty_environment_yields_no_options() {
        assert!(env_credential_options(|_| None).is_empty());
    }

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
