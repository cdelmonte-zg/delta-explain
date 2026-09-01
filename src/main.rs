use std::io::{self, Write};
use std::process::ExitCode;

use clap::Parser;

use delta_explain::error::{Error, Result};
use delta_explain::execution::{self, ExecutionInput};
use delta_explain::gates::GateConfig;
use delta_explain::instrumentation::{Instrumentation, NoOpInstrumentation};
use delta_explain::presentation::{self, OutputFormat, PresentationOptions};
use delta_explain::storage::{self, StorageConfig, StorageOption};
use delta_explain::table;
use delta_explain::table_uri;

#[cfg(feature = "debug-ir")]
use delta_explain::instrumentation::DebugIrObserver;

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

    /// Diagnose why the predicate pruned as it did, with suggestions; in
    /// JSON, adds the "explain" array
    #[arg(long = "explain-why")]
    explain_why: bool,

    /// Write this run's intermediate representations (predicate AST before
    /// and after normalization, classification, lowered kernel predicates,
    /// partition-literal evaluation, survivor counts, kernel trace) to
    /// FILE. Diagnostic output: the format is unstable and outside the
    /// CLI/JSON contract.
    #[cfg(feature = "debug-ir")]
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
        Ok(exit_code) => exit_code,

        Err(err) => {
            eprintln!("Error: {err:#}");

            ExitCode::FAILURE
        }
    }
}

fn try_main() -> Result<ExitCode> {
    let cli = Cli::parse();

    let start = std::time::Instant::now();

    let table_url = table_uri::parse(&cli.path)?;

    let custom_options = cli
        .options
        .iter()
        .map(|raw| raw.parse::<StorageOption>())
        .collect::<Result<Vec<_>>>()?;

    let runtime = storage::open(
        &table_url,
        &StorageConfig {
            env_credentials: cli.env_creds,

            profile: cli.profile.clone(),

            region: cli.region.clone(),

            public: cli.public,

            options: custom_options,
        },
    )?;

    let mut instrumentation = build_instrumentation(&cli)?;

    instrumentation.invocation(&cli.path, cli.predicate.as_deref())?;

    let table = table::open(
        &table_url,
        &runtime.store,
        runtime.engine.as_ref(),
        table::OpenOptions {
            version: cli.at_version,
        },
        instrumentation.as_mut(),
    )?;

    let result = execution::execute(
        ExecutionInput {
            table_path: &cli.path,

            predicate: cli.predicate.as_deref(),

            gate_config: GateConfig {
                min_pruning: cli.min_pruning,

                assert_stats: cli.assert_stats,
            },
        },
        &table,
        runtime.engine.as_ref(),
        instrumentation.as_mut(),
    )?;

    let elapsed_ms = start.elapsed().as_millis();

    instrumentation.finish()?;

    let presentation = presentation::build(
        &result.report,
        &result.gates,
        &table.metadata.baseline,
        elapsed_ms,
        PresentationOptions {
            verbose: cli.verbose,

            limit: cli.limit,

            explain_why: cli.explain_why,
        },
    );

    for failure in presentation::gate_failures(&presentation) {
        eprintln!("{failure}");
    }

    let format = match cli.format.as_str() {
        "json" => OutputFormat::Json,

        _ => OutputFormat::Text,
    };

    let output = format.render(&presentation)?;

    write_stdout(&output)?;

    let exit_code = if result.gates.failed() {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    };

    Ok(exit_code)
}

fn write_stdout(output: &str) -> Result<()> {
    let stdout = io::stdout();

    let mut stdout = stdout.lock();

    if let Err(err) = stdout.write_all(output.as_bytes()) {
        return handle_stdout_error(err);
    }

    if let Err(err) = stdout.flush() {
        return handle_stdout_error(err);
    }

    Ok(())
}

fn handle_stdout_error(err: io::Error) -> Result<()> {
    if err.kind() == io::ErrorKind::BrokenPipe {
        Ok(())
    } else {
        Err(Error::Output(err))
    }
}

fn build_instrumentation(cli: &Cli) -> Result<Box<dyn Instrumentation>> {
    #[cfg(feature = "debug-ir")]
    if let Some(path) = cli.debug_ir.as_deref() {
        return Ok(Box::new(DebugIrObserver::create(path)?));
    }

    #[cfg(not(feature = "debug-ir"))]
    let _ = cli;

    Ok(Box::new(NoOpInstrumentation))
}
